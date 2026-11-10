from __future__ import annotations

import ast
import csv
import json
import pathlib
from pathlib import Path
from typing import Annotated, Any

import tomlkit
from langchain_core.runnables import RunnableConfig
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.types import Command, interrupt

from langgraph.prebuilt import InjectedState, ToolRuntime

from lerai.error_truncation import truncate_with_marker
from lerai.override_agent.knowledge_base import search_leroy_knowledge_base
from lerai.overrides_pipeline.conflict_detector import detect_conflicts, find_invalid_mapnames
from lerai.overrides_pipeline.toml_generator import (
    build_toml_string,
    execute_ast_update,
    validate_stanza,
)
from lerai.overrides_pipeline.entity_extractor import extract_intent

from lerai.api_clients.override_api import (
    deploy_override,
    fetch_offline_prod_csv_diff,
    fetch_override_and_token,
    submit_offline_override,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_PROJECT_ROOT = PROJECT_ROOT
DATA_DIR = pathlib.Path(PROJECT_ROOT) / "lerai" / "data"
OVERRIDE_SCHEMA_PATH = PROJECT_ROOT / "override_schema.json"
SCHEMA_PATH = _PROJECT_ROOT / "lerai" / "prompts" / "leroy_override_entity_extractor_tool.json"


def _load_override_schema() -> dict[str, Any]:
    with OVERRIDE_SCHEMA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


@tool
def extract_override_intent(synthesized_request: str) -> str:
    """
    STEP 1 TOOL. ALWAYS use this tool FIRST. 
    You must pass a FULLY RESOLVED, context-rich natural language request here. 
    If the user provides a short follow-up (e.g., 'change it to 80%'), you MUST 
    synthesize it with the previous context (e.g., 'change quota to 80% for map w5 in region 50565') 
    before passing it to this tool.
    
    Returns a JSON string of the extracted LeROY intent.
    """
    try:
        intent_dict = extract_intent(synthesized_request)
        return json.dumps(intent_dict)
    except Exception as exc:
        return json.dumps({"ok": False, "error_type": "ExtractionError", "details": str(exc)})


@tool
def refresh_live_override_snapshot(runtime: ToolRuntime) -> Command:
    """STEP 3 TOOL. Use this tool exactly once per editing session BEFORE checking conflicts. It fetches the latest live override.toml and concurrency token from the server."""
    try:
        token, current_toml = fetch_override_and_token()
        return Command(
            update={
                "base_override_token": token,
                "live_override_toml": current_toml,
                "messages": [
                    ToolMessage(
                        content="Successfully fetched live override snapshot.",
                        tool_call_id=runtime.tool_call_id,
                    )
                ],
            }
        )
    except Exception as exc:
        error_payload = {
            "ok": False,
            "error_type": "FetchError",
            "details": str(exc),
        }
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        content=json.dumps(error_payload),
                        tool_call_id=runtime.tool_call_id or "",
                    )
                ]
            }
        )


@tool
def detect_override_conflicts(
    intent_json: str,
    live_override_toml: Annotated[str, InjectedState("live_override_toml")],
) -> str:
    """
    STEP 3 TOOL. Pass the JSON string output from extract_override_intent here.
    Reads override.toml and detects if this new intent conflicts with live records.
    """
    try:
        if not live_override_toml:
            raise RuntimeError(
                "Missing live override snapshot. Run refresh_live_override_snapshot before detect_override_conflicts."
            )
        new_intent = json.loads(intent_json)
        invalid_mapnames = find_invalid_mapnames(new_intent)
        warnings: list[str] = []
        if invalid_mapnames:
            warnings.append(
                "Invalid map name(s) provided: "
                + ", ".join(invalid_mapnames)
                + ". These map names are not present in lerai/data/maps.csv."
            )
        
        # Catch extraction errors before running detection
        if "error" in new_intent:
            message = new_intent["error"]
            if warnings:
                message = f"{message} Warning: {' '.join(warnings)}"
            response_dict = {
                "has_conflict": False,
                "message": message,
                "conflicts": [],
                "warnings": warnings,
                "invalid_mapnames": invalid_mapnames,
            }
            return json.dumps(response_dict)
            
        # Call the upgraded semantic conflict detector
        found_conflicts = detect_conflicts(new_intent, live_override_toml)

        status_message = (
            f"Detected {len(found_conflicts)} potential conflict(s)."
            if found_conflicts
            else "No conflicts detected. Safe to proceed."
        )
        if warnings:
            status_message = f"{status_message} Warning: {' '.join(warnings)}"
        
        if found_conflicts:
            response_dict = {
                "has_conflict": True,
                "conflicts": found_conflicts,
                "message": status_message,
                "warnings": warnings,
                "invalid_mapnames": invalid_mapnames,
            }
        else:
            response_dict = {
                "has_conflict": False,
                "message": status_message,
                "conflicts": [],
                "warnings": warnings,
                "invalid_mapnames": invalid_mapnames,
            }
        return json.dumps(response_dict)

    except Exception as exc:
        return json.dumps({"ok": False, "error_type": "ConflictDetectionError", "details": str(exc)})

@tool
def generate_and_validate_toml(intent_json: str) -> dict[str, Any]:
    """
    STEP 2 TOOL. Pass the JSON string output from extract_override_intent here.
    Generates the final TOML code and validates it against the schema.
    Run this before conflict detection so the full draft is available at approval time.
    """
    try:
        intent = json.loads(intent_json)
        
        if "error" in intent:
            return {"ok": False, "toml": "", "error": intent["error"]}
            
        toml_text = build_toml_string(intent)
        schema = _load_override_schema()
        validate_stanza(toml_text, schema)
        return {
            "ok": True,
            "toml": toml_text,
            "error": "",
        }
    except Exception as exc:
        return {"ok": False, "toml": "", "error_type": "TomlGenerationError", "details": str(exc)}


@tool
def search_leroy_documentation(query: str) -> str:
    """Use this tool to search the LeRoy manuals when the user asks a conceptual question about override directives, safety rules, architecture, constraints, or configurations.
    IMPORTANT: Provide short, natural language questions for your query (e.g., 'What is the maximum allowed value for Quota-pct?') rather than a list of keywords."""
    return search_leroy_knowledge_base(query)


@tool
def lookup_infrastructure_data(target_output: str, source_value: str) -> str:
    """Use this tool to look up infrastructure mappings via a flat in-memory table.
    - target_output: What you want to find (must be one of: 'geos', 'countries', 'metros', 'regions').
    - source_value: One or more known entities, comma-separated (e.g., 'France, FR, LAX')."""
    def normalize(s: str) -> str:
        return (s or "").strip().lower().replace(" ", "_")

    def load_csv_rows(file_name: str) -> list[dict[str, str]]:
        csv_path = DATA_DIR / file_name
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    kind = normalize(target_output)
    if kind not in {"geos", "countries", "metros", "regions"}:
        return "Invalid target_output. Use one of: geos, countries, metros, regions."

    # Lightweight country aliases for common natural-language inputs.
    country_aliases = {
        "france": "fr",
        "united_states": "us",
        "united_states_of_america": "us",
        "usa": "us",
        "uk": "gb",
        "united_kingdom": "gb",
        "great_britain": "gb",
        "germany": "de",
        "spain": "es",
        "italy": "it",
        "canada": "ca",
        "australia": "au",
        "india": "in",
        "japan": "jp",
        "china": "cn",
        "brazil": "br",
    }

    source_items = [(part or "").strip() for part in (source_value or "").split(",")]
    valid_source_items = [item for item in source_items if item]
    if not valid_source_items:
        return "Please provide a non-empty source_value to search."

    try:
        geo_country_rows = load_csv_rows("geo_country.csv")
        country_metro_rows = load_csv_rows("country_metro.csv")
        metro_region_rows = load_csv_rows("metro_region.csv")

        geos_by_country: dict[str, list[str]] = {}
        for row in geo_country_rows:
            country = (row.get("country") or "").strip()
            geo = (row.get("geo") or "").strip()
            if not country:
                continue
            geos_by_country.setdefault(country, [])
            if geo and geo not in geos_by_country[country]:
                geos_by_country[country].append(geo)

        metros_by_country: dict[str, list[dict[str, str]]] = {}
        for row in country_metro_rows:
            country = (row.get("country") or "").strip()
            if not country:
                continue
            metro_entry = {
                "metro_area": (row.get("metro_area") or "").strip(),
                "airport_code": (row.get("airport_code") or "").strip(),
            }
            metros_by_country.setdefault(country, []).append(metro_entry)

        regions_by_metro: dict[str, list[str]] = {}
        for row in metro_region_rows:
            metro = (row.get("metro_area") or row.get("metro") or "").strip()
            region = (row.get("region") or "").strip()
            if not metro:
                continue
            regions_by_metro.setdefault(metro, [])
            if region and region not in regions_by_metro[metro]:
                regions_by_metro[metro].append(region)

        all_countries = set(geos_by_country.keys()) | set(metros_by_country.keys())

        flat_table: list[dict[str, str]] = []
        for country in all_countries:
            country_geos = geos_by_country.get(country, [""]) or [""]
            country_metros = metros_by_country.get(country, [{"metro_area": "", "airport_code": ""}])

            for geo in country_geos:
                for metro_entry in country_metros:
                    metro_area = metro_entry.get("metro_area", "")
                    airport_code = metro_entry.get("airport_code", "")
                    metro_regions = regions_by_metro.get(metro_area, [""]) if metro_area else [""]

                    for region in metro_regions:
                        flat_table.append(
                            {
                                "geo": geo,
                                "country": country,
                                "metro_area": metro_area,
                                "airport_code": airport_code,
                                "region": region,
                            }
                        )

        target_column = {
            "geos": "geo",
            "countries": "country",
            "metros": "metro_area",
            "regions": "region",
        }[kind]

        formatted_results: list[str] = []
        for raw_item in valid_source_items:
            normalized_item = normalize(raw_item)
            resolved_item = country_aliases.get(normalized_item, normalized_item)

            item_results: set[str] = set()
            for row in flat_table:
                searchable_values = {
                    normalize(row.get("geo", "")),
                    normalize(row.get("country", "")),
                    normalize(row.get("metro_area", "")),
                    normalize(row.get("airport_code", "")),
                    normalize(row.get("region", "")),
                }
                if resolved_item in searchable_values:
                    target_value = (row.get(target_column) or "").strip()
                    if target_value:
                        item_results.add(target_value)

            if item_results:
                sorted_results = sorted(item_results)
                formatted_results.append(f"{raw_item} -> {', '.join(sorted_results)}")
            else:
                formatted_results.append(f"{raw_item} -> No mapping found")

        return "\n".join(formatted_results)

    except Exception as exc:
        return json.dumps({"ok": False, "error_type": "LookupError", "details": str(exc)})


@tool
def get_unique_infrastructure_values(entity_type: str) -> str:
    """Use this tool to get a comprehensive list of all active entity types in the LeROY network. Valid entity_types are 'geos', 'countries', and 'metros'. Use this to discover what data actually exists before attempting to filter or group them."""
    kind = (entity_type or "").strip().lower()
    if kind not in {"geos", "countries", "metros"}:
        return "Invalid entity_type. Use one of: geos, countries, metros."

    try:
        if kind in {"geos", "countries"}:
            file_path = DATA_DIR / "geo_country.csv"
            with file_path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                column = "geo" if kind == "geos" else "country"
                values = sorted(
                    {
                        (row.get(column) or "").strip()
                        for row in reader
                        if (row.get(column) or "").strip()
                    }
                )
            return ", ".join(values) if values else f"No {kind} found."

        file_path = DATA_DIR / "country_metro.csv"
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            values = sorted(
                {
                    (row.get("metro_area") or "").strip()
                    for row in reader
                    if (row.get("metro_area") or "").strip()
                }
            )
        return ", ".join(values) if values else "No metros found."
    except Exception as exc:
        return json.dumps({"ok": False, "error_type": "LookupError", "details": str(exc)})


@tool
def lookup_directive_schema(directive_name: str) -> str:
    """Use this tool to find the exact structural limitations, allowed enum values, data types, and min/max bounds for a specific override directive (e.g., 'Quota-pct', 'Access-control'). Do NOT guess constraints; use this tool."""
    name = (directive_name or "").strip()
    if not name:
        return "Please provide a non-empty directive_name."

    try:
        with SCHEMA_PATH.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        directives = (
            schema.get("parameters", {})
            .get("properties", {})
            .get("Override-Directive", {})
            .get("properties", {})
        )

        if not isinstance(directives, dict):
            return "Directive schema structure is invalid or missing 'Override-Directive.properties'."

        lower_to_actual = {str(key).lower(): key for key in directives.keys()}
        matched_key = lower_to_actual.get(name.lower())
        if not matched_key:
            return f"Directive '{directive_name}' does not exist in the schema."

        return json.dumps(directives[matched_key], indent=2)
    except Exception as exc:
        return json.dumps({"ok": False, "error_type": "LookupError", "details": str(exc)})


def _load_conflict_rules() -> dict[str, Any]:
    """Loads the LeROY override conflict rules JSON from the prompts directory."""
    rules_path = _PROJECT_ROOT / "lerai" / "prompts" / "leroy_override_conflict_rules.json"
    with rules_path.open("r", encoding="utf-8") as f:
        return json.load(f)


@tool
def apply_override_to_workspace(
    new_intents_json: str,
    target_intents_json: str,
    live_override_toml: Annotated[str, InjectedState("live_override_toml")],
    runtime: ToolRuntime,
    draft_toml: Annotated[str | None, InjectedState("draft_toml")],
) -> Command:
    """
    STEP 6 TOOL. Use this tool ONLY after the user explicitly approves the deployment of the override.
    Reads the live override.toml from the workspace, nukes any target records, appends one or more
    new intents, and saves the file back to disk.

    `new_intents_json` accepts either:
    - A single JSON object (backward-compatible), or
    - A JSON list of objects for multi-stanza append.
    """

    def _flatten_intent(intent: dict[str, Any]) -> dict[str, Any]:
        """Flattens nested JSON payloads into a flat dictionary for TOML."""
        flat = {}
        for k, v in intent.items():
            if isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    flat[sub_k] = sub_v
            else:
                flat[k] = v
        return flat

    def _parse_payload(payload_str: str) -> list[dict[str, Any]]:
        """
        Robustly parses a payload string that may be JSON or TOML, with optional Markdown wrapping.
        
        Returns:
            list[dict[str, Any]]: A list of parsed records.
            
        Raises:
            ValueError: If the payload cannot be parsed as JSON or TOML.
        """
        # Handle empty or None payloads
        if not payload_str or not payload_str.strip():
            return []
        
        # Strip Markdown code block wrappers
        cleaned = payload_str.strip()
        lines = cleaned.split('\n')
        
        # Remove opening code block marker (e.g., ```json, ```toml, ```)
        if lines and lines[0].strip().startswith('```'):
            lines = lines[1:]
        
        # Remove closing code block marker
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        
        cleaned = '\n'.join(lines).strip()
        
        if not cleaned:
            return []
        
        # Try parsing as JSON first
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                # If the LLM wrapped the array in a single key like {"to_delete": [...]}, unwrap it
                if len(parsed) == 1 and isinstance(list(parsed.values())[0], list):
                    return list(parsed.values())[0]
                # Otherwise, treat it as a single flat record
                return [parsed]
            elif isinstance(parsed, list):
                return parsed
            else:
                raise ValueError(
                    f"Expected JSON object or array, got {type(parsed).__name__}"
                )
        except json.JSONDecodeError:
            pass
        
        # Fall back to TOML parsing
        try:
            doc = tomlkit.parse(cleaned)
            if "override-records" not in doc:
                raise ValueError(
                    "TOML payload missing required 'override-records' key"
                )
            
            records = doc["override-records"]
            if not isinstance(records, list):
                raise ValueError(
                    f"Expected 'override-records' to be an array, got {type(records).__name__}"
                )
            
            # Convert tomlkit objects to standard Python dicts
            result = []
            for record in records:
                if hasattr(record, 'items'):
                    result.append(dict(record))
                else:
                    result.append(record)
            return result
        except Exception as toml_exc:
            raise ValueError(
                f"Failed to parse payload as JSON or TOML: {toml_exc}"
            )

    try:
        raw_new_intents = _parse_payload(new_intents_json)
        
        # FIX 1: ONLY flatten the new intents so they don't write as nested TOML
        new_intents = [_flatten_intent(intent) for intent in raw_new_intents]

        raw_target_intents = _parse_payload(target_intents_json)

        # FIX 2: Apply _flatten_intent() to extracted items before appending
        target_intents = []
        for item in raw_target_intents:
            if isinstance(item, dict) and "record" in item:
                extracted = item["record"]
            else:
                extracted = item
            target_intents.append(_flatten_intent(extracted))

        try:
            base_toml = draft_toml or live_override_toml
            if not base_toml or not base_toml.strip():
                return Command(update={"messages": [ToolMessage(
                    content=json.dumps({
                        "ok": False,
                        "error_type": "MissingSnapshot",
                        "details": "No live override loaded. Call refresh_live_override_snapshot first, then retry.",
                    }),
                    tool_call_id=runtime.tool_call_id or "",
                )]})

            raw_new_intents = _parse_payload(new_intents_json)
            new_intents = [_flatten_intent(intent) for intent in raw_new_intents]
            
            doc = tomlkit.parse(base_toml)
            
        except Exception as exc:
            tool_message = ToolMessage(
                content=json.dumps({
                    "ok": False,
                    "error_type": "ApplyOverrideError",
                    "details": f"{type(exc).__name__}: {exc or 'no detail'}",
                }),
                tool_call_id=runtime.tool_call_id or "",
            )
            return Command(update={"messages": [tool_message]})
        conflict_rules = _load_conflict_rules()

        doc = execute_ast_update(doc, target_intents, new_intents, conflict_rules)

        updated_toml = tomlkit.dumps(doc)
        tool_message = ToolMessage(
            content=json.dumps({"ok": True, "message": "Successfully applied updates in-memory."}),
            tool_call_id=runtime.tool_call_id or "",
        )
        return Command(
            update={
                "draft_toml": updated_toml,
                # A new draft makes any earlier offline run stale.
                "offline_diff_report_shown": False,
                "messages": [tool_message],
            },
        )
    except Exception as exc:
        tool_message = ToolMessage(
            content=json.dumps({"ok": False, "error_type": "ApplyOverrideError", "details": f"Failed to apply overrides in-memory: {exc}"}),

            tool_call_id=runtime.tool_call_id or "",
        )
        return Command(
            update={"messages": [tool_message]},
        )

def _compact_deploy_result(response: dict[str, Any]) -> dict[str, Any]:
    """Reduce deploy payload to minimal fields for LLM context safety."""
    raw_success = response.get("success")
    if isinstance(raw_success, bool):
        success = raw_success
    elif isinstance(raw_success, str):
        success = raw_success.strip().lower() == "true"
    else:
        success = bool(raw_success)

    def _as_str(value: Any) -> str | None:
        return None if value is None else str(value)

    # `token` is the override revision token written by this deploy.
    override_token = _as_str(response.get("offline_override_token"))

    # `git_leroy_output_token` is the offline-vs-production diff token.
    diff_token = _as_str(response.get("git_leroy_output_token"))

    # Legacy fallback: older payloads nested the diff token inside `offline_diff`.
    if diff_token is None:
        offline_diff = response.get("offline_diff")
        if isinstance(offline_diff, str):
            try:
                parsed_diff = ast.literal_eval(offline_diff)
            except (ValueError, SyntaxError):
                parsed_diff = None
            if isinstance(parsed_diff, dict):
                diff_token = _as_str(parsed_diff.get("token"))
        elif isinstance(offline_diff, dict):
            diff_token = _as_str(offline_diff.get("token"))

    compact_payload: dict[str, Any] = {
        "ok": success,
        "success": success,
        "returncode": response.get("returncode"),
    }
    if override_token is not None:
        compact_payload["offline_override_token"] = override_token
    if diff_token is not None:
        compact_payload["git_leroy_output_token"] = diff_token

    diff_returncode = response.get("diff_returncode")
    if diff_returncode is not None:
        compact_payload["diff_returncode"] = diff_returncode
        if diff_returncode != 0:
            compact_payload["diff_stderr"] = truncate_with_marker(
                response.get("diff_stderr", ""), "diff_stderr"
            )

    if not success:
        compact_payload["message"] = truncate_with_marker(
            response.get("message", ""), "message"
        )
        compact_payload["offline_run_errors"] = truncate_with_marker(
            response.get("offline_run_errors", []), "offline_run_errors"
        )
        compact_payload["stderr"] = truncate_with_marker(
            response.get("stderr", ""), "stderr"
        )
        offline_run_log = response.get("offline_run_log")
        compact_payload["offline_run_log"] = truncate_with_marker(
            offline_run_log if isinstance(offline_run_log, str) else "",
            "offline_run_log",
        )

    return compact_payload

def _extract_inline_diff(response: dict[str, Any]) -> dict[str, Any] | None:
    """Return the {"token", "diffs"} payload embedded in an offline-run response."""
    offline_diff = response.get("offline_diff")
    if isinstance(offline_diff, str) and offline_diff.strip():
        try:
            offline_diff = ast.literal_eval(offline_diff.strip())
        except (ValueError, SyntaxError):
            offline_diff = json.loads(offline_diff)
    if isinstance(offline_diff, dict) and offline_diff.get("diffs") is not None:
        return offline_diff
    if response.get("diffs") is not None:
        return {
            "token": response.get("git_leroy_output_token"),
            "diffs": response.get("diffs"),
        }
    return None


@tool
def deploy_and_trigger_offline_computation(
    runtime: ToolRuntime,
    draft_toml: Annotated[str, InjectedState("draft_toml")],
    base_override_token: Annotated[str, InjectedState("base_override_token")],
) -> Command:
    """
    STEP 7 TOOL. Submit the drafted override TOML, trigger offline computation, and
    return the offline-vs-production CSV diff.

    On success the result contains a `report_markdown` field. It will display a 
    human-readable summary of the offline-vs-production diff, including structural and quota changes.
    On failure the result contains an `error_type` and `details` field describing the failure.
    """
    try:
        response = submit_offline_override(
            updated_toml=draft_toml,
            base_token=base_override_token,
        )
        compact_response = _compact_deploy_result(response)

        # Close the deploy gate; it reopens only once this run's diff is rendered.
        state_update: dict[str, Any] = {"offline_diff_report_shown": False}
        if compact_response.get("offline_override_token"):
            state_update["override_token"] = compact_response["offline_override_token"]
        if compact_response.get("git_leroy_output_token"):
            state_update["git_leroy_output_token"] = compact_response[
                "git_leroy_output_token"
            ]

        # Render the inline diff returned alongside the deploy result. The endpoint
        # nests it as `offline_diff` (a Python-literal string or a dict); older
        # payloads put `diffs` at the top level.
        diff_payload = _extract_inline_diff(response)
        if compact_response.get("ok") and diff_payload is not None:
            try:
                compact_response["report_markdown"] = _render_offline_prod_diff_report(
                    diff_payload
                )
                # Mark the diff as presented so the deploy tool's gate opens.
                state_update["offline_diff_report_shown"] = True
            except Exception as exc:
                compact_response["report_error"] = truncate_with_marker(
                    str(exc), "report_error"
                )

        state_update["messages"] = [
            ToolMessage(
                content=json.dumps(compact_response),
                tool_call_id=runtime.tool_call_id or "",
            )
        ]
        return Command(update=state_update)
    except Exception as exc:
        tool_message = ToolMessage(
            content=json.dumps(
                {
                    "ok": False,
                    "error_type": "DeploymentError",
                    "details": truncate_with_marker(str(exc), "details"),
                }
            ),
            tool_call_id=runtime.tool_call_id or "",
        )
        # A failed run must not leave an earlier run's approval gate open.
        return Command(
            update={"offline_diff_report_shown": False, "messages": [tool_message]}
        )


def _format_timestamp(value: Any) -> str:
    """Render an ISO timestamp (or missing value) for the report."""
    if not value:
        return "_not available_"
    return str(value)


def _format_structure_diffs(rows: list[dict[str, Any]]) -> list[str]:
    """Render structure diffs grouped by change type as markdown bullets."""
    if not rows:
        return ["  - _No structural differences._"]

    added = [row for row in rows if str(row.get("type", "")).lower() == "added"]
    deleted = [row for row in rows if str(row.get("type", "")).lower() == "deleted"]
    other = [row for row in rows if row not in added and row not in deleted]

    lines: list[str] = []
    for label, group in (("Added", added), ("Deleted", deleted), ("Other", other)):
        if not group:
            continue
        lines.append(f"  - **{label} ({len(group)})**")
        for row in group:
            region = row.get("region", "?")
            item = row.get("item", "?")
            info = str(row.get("info", "")).strip()
            suffix = f" — {info}" if info else ""
            lines.append(f"    - region `{region}` | item `{item}`{suffix}")
    return lines


def _format_number(value: Any) -> str:
    """Render numeric quota values without floating-point noise."""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return f"{round(float(value), 2):g}"
    return str(value)


def _format_quota_diffs(rows: list[dict[str, Any]]) -> list[str]:
    """Render quota diffs as a markdown table sorted by largest delta."""
    if not rows:
        return ["  - _No quota differences._"]

    def _delta(row: dict[str, Any]) -> float:
        try:
            return abs(float(row.get("diff", 0)))
        except (TypeError, ValueError):
            return 0.0

    ordered = sorted(rows, key=_delta, reverse=True)

    lines = [
        f"  - **{len(ordered)} quota change(s)**",
        "",
        "    | Region | Map | Offline | Production | Diff |",
        "    | --- | --- | ---: | ---: | ---: |",
    ]
    for row in ordered:
        region = row.get("region", "?")
        identifier = row.get("map_identifier", row.get("item", "?"))
        offline = _format_number(row.get("manual_quota", row.get("offline_quota", "?")))
        production = _format_number(row.get("official_quota", row.get("production_quota", "?")))
        diff = _format_number(row.get("diff", "?"))
        lines.append(
            f"    | {region} | {identifier} | {offline} | {production} | {diff} |"
        )
    lines.append("")
    return lines


def _render_offline_prod_diff_report(raw_payload: Any) -> str:
    """Build a human-readable markdown report from the diff API payload."""

    def _normalize_entries(payload: Any) -> tuple[list[dict[str, Any]], str | None]:
        """
        Returns:
            (entries, token)
        """
        token: str | None = None

        # 1) Wrapped HTTP payload form
        # {"returncode":0, "stdout":"{'token':'...','diffs':{...}}", "stderr":""}
        if isinstance(payload, dict) and "stdout" in payload:
            if payload.get("returncode") not in (0, None):
                raise ValueError(
                    f"offline_prod_csv_diff failed (returncode={payload.get('returncode')}): "
                    f"{payload.get('stderr') or payload.get('message') or 'unknown error'}"
                )

            stdout_val = payload.get("stdout", "")
            if isinstance(stdout_val, str) and stdout_val.strip():
                text = stdout_val.strip()
                try:
                    payload = ast.literal_eval(text)   # current endpoint style
                except (ValueError, SyntaxError):
                    payload = json.loads(text)         # fallback if backend switches to JSON
            else:
                payload = {}

        # 2) Parsed stdout payload form
        # {"token":"...", "diffs":{"blc.csv": {...}, "fcs.csv": {...}}}
        if isinstance(payload, dict):
            maybe_token = payload.get("token")
            if maybe_token is not None:
                token = str(maybe_token)

            if "diffs" in payload and isinstance(payload["diffs"], dict):
                payload = payload["diffs"]

        # 3) Mapping filename -> entry
        if isinstance(payload, dict):
            # Single-entry dict form
            if "filename" in payload:
                return [payload], token

            entries: list[dict[str, Any]] = []
            for filename, entry in payload.items():
                if isinstance(entry, dict):
                    row = dict(entry)
                    row.setdefault("filename", filename)
                    entries.append(row)
            return entries, token

        # 4) List[dict] form
        if isinstance(payload, list):
            return [e for e in payload if isinstance(e, dict)], token

        return [], token

    entries, token = _normalize_entries(raw_payload)

    if not entries:
        return "### 📊 Offline vs Production CSV Diff\n\n_No files returned by the diff API._"

    total_structure = 0
    total_quota = 0
    blocks: list[str] = ["### 📊 Offline vs Production CSV Diff", ""]

    for entry in entries:
        filename = entry.get("filename", "<unknown file>")

        structure_diffs = entry.get("structure_diffs")
        quota_diffs = entry.get("quota_diffs")
        if not isinstance(structure_diffs, list):
            structure_diffs = []
        if not isinstance(quota_diffs, list):
            quota_diffs = []

        total_structure += len(structure_diffs)
        total_quota += len(quota_diffs)

        blocks.append(f"#### 📄 `{filename}`")
        blocks.append(
            "- **Offline last modified:** "
            + _format_timestamp(entry.get("offline_last_modified"))
        )
        blocks.append(
            "- **Production last modified:** "
            + _format_timestamp(entry.get("production_last_modified"))
        )
        blocks.append(f"- **Structure diffs ({len(structure_diffs)}):**")
        blocks.extend(_format_structure_diffs(structure_diffs))
        blocks.append(f"- **Quota diffs ({len(quota_diffs)}):**")
        blocks.extend(_format_quota_diffs(quota_diffs))
        blocks.append("")

    blocks.append(
        f"**Summary:** {len(entries)} file(s), "
        f"{total_structure} structural change(s), {total_quota} quota change(s)."
    )

    if token:
        blocks.append("")
        blocks.append(f"**Diff token:** `{token}`")

    return "\n".join(blocks)


@tool
def request_deployment_approval(stanzas_to_add: str, stanzas_to_delete: str, message: str = "") -> str:
    """
    STEP 4 TOOL. You MUST use this tool after you finish drafting the TOML and resolving conflicts,
    and BEFORE you call apply_override_to_workspace.
    It shows the user the exact changes and pauses execution to wait for their approval or feedback.
    Use the `message` parameter to pass any warnings, conflict notes, or questions to the user.
    """
    parts: list[str] = []

    if message and message.strip():
        parts.append(f"### 💬 Agent Message:\n{message}\n")

    if stanzas_to_delete and stanzas_to_delete.strip():
        parts.append(
            f"### 🚨 To Be Deleted (Nuked):\n```toml\n{stanzas_to_delete}\n```\n"
        )

    parts.append(f"### 🆕 To Be Added:\n```toml\n{stanzas_to_add}\n```\n")

    # parts.append(
    #     '**Do you approve these changes for deployment?**'
    #     ' (Reply "Yes" to deploy, or tell me what to change)'
    # )

    markdown_string = "\n".join(parts)
    user_response = interrupt(markdown_string)
    return user_response


@tool
def deploy_offline_override_record(
    runtime: ToolRuntime,
    draft_toml: Annotated[str, InjectedState("draft_toml")],
    git_leroy_output_token: Annotated[str, InjectedState("git_leroy_output_token")],
    override_token: Annotated[str, InjectedState("override_token")],
    base_override_token: Annotated[str, InjectedState("base_override_token")],
    offline_diff_report_shown: Annotated[
        bool, InjectedState("offline_diff_report_shown")
    ],
) -> Command:
    """
    STEP 8 TOOL. Promotes the offline override record to production.

    Use this ONLY when the user has already been shown the offline-vs-production
    diff report AND then explicitly asks to deploy (e.g. "approve", "deploy the
    offline override record"). This tool will pause and ask the user to confirm
    one final time before anything is promoted. Never call it before the diff
    report has been presented.
    """
    # Guard 1: the diff must actually have been rendered for the user.
    if not offline_diff_report_shown:
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        content=json.dumps(
                            {
                                "ok": False,
                                "error_type": "DiffNotReviewed",
                                "details": (
                                    "The offline-vs-production diff has not been shown "
                                    "to the user yet. Run "
                                    "deploy_and_trigger_offline_computation, present the "
                                    "report, and only then retry the deploy."
                                ),
                            }
                        ),
                        tool_call_id=runtime.tool_call_id or "",
                    )
                ]
            }
        )

    # Guard 2: we need the token identifying the offline run to promote.
    if not git_leroy_output_token:
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        content=json.dumps(
                            {
                                "ok": False,
                                "error_type": "MissingOfflineToken",
                                "details": (
                                    "No git_leroy_output_token in state. The offline "
                                    "computation must complete successfully before deploying."
                                ),
                            }
                        ),
                        tool_call_id=runtime.tool_call_id or "",
                    )
                ]
            }
        )

    # Guard 3: explicit human confirmation at the point of no return.
    confirmation = interrupt(
        "### 🚀 Final Confirmation: Deploy to Production\n\n"
        "You are about to promote the reviewed offline override record to production.\n\n"
        f"- **Offline run token:** `{git_leroy_output_token}`\n"
        f"- **Override revision:** `{override_token or 'unknown'}`\n"
        f"- **Base production revision:** `{base_override_token or 'unknown'}`\n\n"
        "**This action affects live traffic.** Reply `deploy` or `approve` to proceed, "
        "or anything else to cancel."
    )

    approved = str(confirmation or "").strip().lower() in {
        "deploy",
        "approve",
        "approved",
        "yes",
        "y",
        "confirm",
        "confirmed",
    }
    if not approved:
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        content=json.dumps(
                            {
                                "ok": False,
                                "error_type": "DeploymentCancelled",
                                "details": (
                                    "User did not confirm the production deploy. "
                                    f"Response was: {confirmation!r}"
                                ),
                            }
                        ),
                        tool_call_id=runtime.tool_call_id or "",
                    )
                ]
            }
        )

    try:
        response = deploy_override(
            updated_toml=draft_toml,
            git_leroy_output_token=git_leroy_output_token,
            override_token=override_token or None,
            base_token=base_override_token or None,
        )
        compact_response = {
            "ok": True,
            "message": "Offline override record deployed to production.",
            "returncode": response.get("returncode"),
        }
        deploy_message = response.get("message")
        if deploy_message:
            compact_response["deploy_message"] = truncate_with_marker(
                str(deploy_message), "deploy_message"
            )
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        content=json.dumps(compact_response),
                        tool_call_id=runtime.tool_call_id or "",
                    )
                ]
            }
        )
    except Exception as exc:
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        content=json.dumps(
                            {
                                "ok": False,
                                "error_type": "DeployOverrideError",
                                "details": truncate_with_marker(str(exc), "details"),
                            }
                        ),
                        tool_call_id=runtime.tool_call_id or "",
                    )
                ]
            }
        )

SUPERVISOR_TOOLS = [
    extract_override_intent,
    refresh_live_override_snapshot,
    detect_override_conflicts,
    generate_and_validate_toml,
    search_leroy_documentation,
    lookup_infrastructure_data,
    get_unique_infrastructure_values,
    lookup_directive_schema,
    request_deployment_approval,
    apply_override_to_workspace,
    deploy_and_trigger_offline_computation,
    deploy_offline_override_record,
]