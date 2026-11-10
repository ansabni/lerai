#!/usr/bin/env python3
"""
leroy_overrides_writer.py
- Orchestrates the LeRoy overrides generation pipeline.
- Extracts intent, checks for conflicts, and safely generates a validated TOML stanza.
"""

import sys
import os
import argparse
import logging
import json
from pathlib import Path
from typing import Any, Optional
from functools import lru_cache
import base64


from langgraph.types import Command
from lerai.override_agent.graph import get_compiled_graph
from lerai.override_agent.nodes import build_initial_input

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PROMPTS_DIR = PROJECT_ROOT / "lerai" / "prompts"
WRITER_RESPONSE_TEMPLATES_FILE = PROMPTS_DIR / "leroy_override_writer_response_templates.json"
CONFLICT_RULES_FILE = PROMPTS_DIR / "leroy_override_conflict_rules.json"
LOG_FILE_PATH = PROJECT_ROOT / "leroy_override_pipeline.log"

from lerai.logging_utils import redact_value
from lerai.override_agent.graph import get_compiled_graph
from lerai.override_agent.nodes import build_initial_input
from lerai.overrides_pipeline.entity_extractor import extract_intent
from lerai.overrides_pipeline.conflict_detector import detect_conflicts
from lerai.overrides_pipeline.toml_generator import build_toml_string, validate_stanza

logger = logging.getLogger(__name__)

def _get_api(webex_api: Any | None = None) -> Any | None:
    """Helper to ensure we have a working Webex API instance."""
    if webex_api is not None:
        return webex_api
    try:
        WebexTeamsAPI = _load_webex_api()
        token = os.environ.get("WEBEX_ACCESS_TOKEN")
        if token:
            return WebexTeamsAPI(access_token=token)
    except Exception:
        pass
    return None

def _as_json(value: object) -> str:
    """Best-effort pretty serialization for runtime-generated objects."""
    try:
        return json.dumps(value, default=str, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(value)


def configure_pipeline_logging() -> None:
    """Configures human-readable file logging for direct script runs."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    file_handler = logging.FileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s\n%(message)s\n")
    )
    root_logger.addHandler(file_handler)


@lru_cache(maxsize=1)
def _load_json_file(file_path: Path) -> dict:
    """Loads and validates that a JSON file contains an object."""
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Required JSON file not found: {file_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in file: {file_path}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {file_path}")

    return payload


@lru_cache(maxsize=1)
def _load_writer_response_templates() -> dict:
    """Loads response templates used by the override writer."""
    templates = _load_json_file(WRITER_RESPONSE_TEMPLATES_FILE)
    required = [
        "hard_conflict",
        "warning_note",
        "success",
        "error",
    ]
    missing = [key for key in required if key not in templates]
    if missing:
        raise ValueError(
            "Missing writer response templates: " + ", ".join(missing)
        )
    return templates


@lru_cache(maxsize=1)
def _load_no_conflict_message() -> str:
    """Loads the canonical no-conflict message from conflict rules."""
    conflict_rules = _load_json_file(CONFLICT_RULES_FILE)
    messages = conflict_rules.get("messages", {})
    no_conflict = messages.get("no_conflict")
    if not isinstance(no_conflict, str) or not no_conflict:
        raise ValueError("Conflict rules must define messages.no_conflict")
    return no_conflict


def _render_template(template_key: str, **kwargs: str) -> str:
    """Renders a named response template with placeholder values."""
    template = _load_writer_response_templates()[template_key]
    return template.format(**kwargs)

def load_schema() -> dict:
    """Loads the authoritative JSON schema for validation."""
    schema_path = PROJECT_ROOT / "override_schema.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return schema


def _load_webex_api():
    try:
        from webexteamssdk import WebexTeamsAPI
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing required dependency: webexteamssdk") from exc
    return WebexTeamsAPI


def _value_from_message(source: Any, key: str) -> Any:
    if source is None:
        return None
    if isinstance(source, dict):
        return source.get(key)
    return getattr(source, key, None)

def _extract_thread_id(webex_message: Any, webex_api: Any | None = None) -> str:
    """Use parentId if present, native parent block, or ask Webex API with an encoded Global ID."""
    
    # 1. Native WebSocket parent block (catches threaded replies instantly)
    if isinstance(webex_message, dict):
        if "parent" in webex_message and isinstance(webex_message["parent"], dict):
            parent_id = webex_message["parent"].get("id")
            if parent_id:
                return str(parent_id)

    candidates = []
    if isinstance(webex_message, dict):
        if "data" in webex_message:
            candidates.append(webex_message["data"])
        if "message" in webex_message:
            candidates.append(webex_message["message"])
        if "object" in webex_message:
            candidates.append(webex_message["object"])
    candidates.append(webex_message)

    # 2. Standard parentId search
    for candidate in candidates:
        parent_id = _value_from_message(candidate, "parentId")
        if parent_id:
            return str(parent_id)

    # 3. Extract the current message ID
    message_id = None
    for candidate in candidates:
        msg_id = _value_from_message(candidate, "id")
        if msg_id:
            message_id = str(msg_id)
            break

    if not message_id:
        return "default-thread"

    # 4. BULLETPROOF FALLBACK: Ask Webex directly using a Base64 Global ID
    api = _get_api(webex_api)
    if api:
        try:
            # webexteamssdk requires base64 encoded Global IDs. If it's a raw UUID, encode it.
            if not message_id.startswith("Y2lzY2"):  # 'cisc' in base64
                global_id = base64.b64encode(f"ciscospark://us/MESSAGE/{message_id}".encode()).decode('utf-8')
            else:
                global_id = message_id

            real_msg = api.messages.get(global_id)
            if hasattr(real_msg, "parentId") and real_msg.parentId:
                return str(real_msg.parentId)
        except Exception as exc:
            # We don't need to log this verbosely anymore if the 'parent' dict catches it natively
            pass

    return message_id

def _extract_room_id(webex_message: Any) -> str:
    """Extract the room ID from the Webex payload safely."""
    candidates = []
    if isinstance(webex_message, dict):
        if "data" in webex_message:
            candidates.append(webex_message["data"])
        if "message" in webex_message:
            candidates.append(webex_message["message"])
    candidates.append(webex_message)

    for candidate in candidates:
        # 1. Standard message object attribute
        room_id = _value_from_message(candidate, "roomId")
        if room_id:
            return str(room_id)

        # 2. Fallback for Webex 'activity' payloads
        if isinstance(candidate, dict):
            target = candidate.get("target", {})
            if isinstance(target, dict) and target.get("id"):
                return str(target["id"])
                
            space = candidate.get("space", {})
            if isinstance(space, dict) and space.get("id"):
                return str(space["id"])
                
        # 3. Fallback for object-style activity payloads
        target = getattr(candidate, "target", None)
        if target and hasattr(target, "id"):
            return str(target.id)

    return ""

def _coerce_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and "text" in item:
                parts.append(str(item["text"]))
            else:
                parts.append(str(item))
        return "\n".join(part for part in parts if part)

    return str(content)


def _extract_last_ai_markdown(graph_result: dict[str, Any]) -> str:
    messages = graph_result.get("messages", [])
    for message in reversed(messages):
        if _value_from_message(message, "type") == "ai":
            return _coerce_message_content(_value_from_message(message, "content"))
    return ""


def _extract_interrupt_payload(interrupt_obj: Any) -> str:
    """Extracts the human-readable payload from a LangGraph interrupt object."""
    value = _value_from_message(interrupt_obj, "value")
    if value is None and isinstance(interrupt_obj, dict):
        value = interrupt_obj.get("value")
    if value is None:
        value = interrupt_obj
    return str(value)


def _format_approval_interrupt(payload: str) -> str:
    """Formats approval interrupts with the same CLI-friendly action banner."""
    text = (payload or "").strip()
    if not text:
        return ""

    if "ACTION REQUIRED: APPROVAL REQUEST" in text:
        return text

    return "\n".join(
        [
            "⚠️  ACTION REQUIRED: APPROVAL REQUEST",
            text,
        ]
    )


def _format_all_interrupts(interrupts: list[Any]) -> str:
    """Formats all interrupt payloads in order, skipping empty payloads."""
    rendered_blocks: list[str] = []
    for interrupt_obj in interrupts:
        payload = _extract_interrupt_payload(interrupt_obj)
        formatted = _format_approval_interrupt(payload)
        if formatted.strip():
            rendered_blocks.append(formatted)
    return "\n\n".join(rendered_blocks)


def _send_threaded_webex_reply(
    markdown: str,
    thread_id: str,
    webex_message: Any,
    webex_api: Any | None = None,
) -> bool:
    room_id = _extract_room_id(webex_message)
    if not room_id:
        return False

    api = _get_api(webex_api)
    if not api:
        raise ValueError("Missing WEBEX_ACCESS_TOKEN; cannot send threaded Webex reply.")

    api.messages.create(roomId=room_id, markdown=markdown, parentId=thread_id)
    return True

def write_toml(
    user_question: str,
    xml_string: Optional[str] = None,
    webex_message: Any | None = None,
    webex_api: Any | None = None,
) -> Optional[str]:
    """
    Orchestrates override generation through LangGraph and optionally posts
    the final markdown response directly into the originating Webex thread.
    """
    thread_id = "unknown-thread"
    try:
        app = get_compiled_graph()
        
        # Instantiate the API early so we can do the foolproof lookup
        api = _get_api(webex_api)
        
        thread_id = _extract_thread_id(webex_message, webex_api=api) if webex_message is not None else "local-cli"
        config = {"configurable": {"thread_id": thread_id}}

        # Check if this thread is currently paused/interrupted
        current_state = app.get_state(config)
        is_interrupted = len(current_state.next) > 0

        logger.info(
            f"Invoking override graph (Interrupted: {is_interrupted})",
            extra={"thread_id": redact_value(thread_id)},
        )

        if is_interrupted:
            graph_result = app.invoke(Command(resume=user_question), config=config)
        else:
            state = build_initial_input(user_question, jira_xml=xml_string)
            graph_result = app.invoke(state, config=config)

        logger.info(
            f"Override graph completed (Interrupted: {is_interrupted})",
            extra={"thread_id": redact_value(thread_id)},
        )

        interrupts = graph_result.get("__interrupt__")
        if interrupts:
            source = "interrupt"
            final_response = _format_all_interrupts(interrupts)
        else:
            source = "ai_message"
            final_response = _extract_last_ai_markdown(graph_result)

        logger.info(
            f"Response resolved (source={source}, length={len(final_response)})",
            extra={"thread_id": redact_value(thread_id)},
        )

        if not final_response:
            logger.error(
                f"Empty response from {source}; "
                f"result_keys={sorted(graph_result.keys())}, "
                f"message_types={[_value_from_message(m, 'type') for m in graph_result.get('messages', [])]}",
                extra={"thread_id": redact_value(thread_id)},
            )
            raise ValueError(f"Override graph returned no AI response (source={source})")

        logger.info(f"Final response generated", extra={"thread_id": redact_value(thread_id)})

        if webex_message is not None:
            sent = _send_threaded_webex_reply(
                markdown=final_response,
                thread_id=thread_id,
                webex_message=webex_message,
                webex_api=api,
            )
            if not sent:
                raise ValueError("Unable to resolve roomId for threaded Webex reply.")
            return None

        return final_response
    except Exception as exc:
        logger.exception(
            f"Override pipeline failed: {exc} | status={getattr(exc, 'status_code', None)} | body={getattr(exc, 'body', None)}",
            extra={"thread_id": redact_value(thread_id)},
        )
        return _render_template("error", error_message=str(exc))

    #except Exception as exc:
    #    logger.error("Override pipeline failed", extra={"error": redact_value(str(exc))})
    #    return _render_template("error", error_message=str(exc))
    #    return _render_template("error", error_message=str(exc))

def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a Leroy override TOML stanza")
    parser.add_argument(
        "user_question",
        nargs="?",
        default="I'd like to remove mm2 from all the US large regions. Ticket is LEROYOPS-99.",
        help="Ticket/request description to convert into a TOML override stanza",
    )
    return parser.parse_args(argv)

if __name__ == "__main__":
    configure_pipeline_logging()
    args = _parse_args(sys.argv[1:])
    try:
        # Test the end-to-end pipeline
        output = write_toml(args.user_question)
        print(output)
        logger.info("Manual override writer output generated")
    except Exception as exc:
        print(f"Critical error: {exc}", file=sys.stderr)
        sys.exit(1)