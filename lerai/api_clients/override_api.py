from __future__ import annotations

import ast
import logging
import os
import ast
import json
import logging
import os
from typing import Any, Tuple

import requests

from lerai.logging_utils import redact_value
from lerai.error_truncation import truncate_with_marker

logger = logging.getLogger(__name__)

DEFAULT_HTTP_TIMEOUT_SECONDS = 900.0

def _log_api_request(method: str, url: str, payload: dict[str, Any] | None = None) -> None:
    redacted_url = redact_value(url)
    redacted_payload = redact_value(payload) if payload is not None else None
    logger.info(
        "Override API request method=%s url=%s payload=%s",
        method,
        redacted_url,
        redacted_payload,
        extra={
            "method": method,
            "url": redacted_url,
            "payload": redacted_payload,
        },
    )

def _log_api_response(response: requests.Response, payload: Any | None = None) -> None:
    payload_value = redact_value(payload) if payload is not None else None
    raw_body_preview = None
    if payload is None:
        raw_body_preview = redact_value(response.text[:2000])

    logger.info(
        "Override API response status=%s url=%s reason=%s payload=%s raw_body_preview=%s",
        response.status_code,
        redact_value(response.url),
        redact_value(response.reason),
        payload_value,
        raw_body_preview,
        extra={
            "status_code": response.status_code,
            "url": redact_value(response.url),
            "reason": redact_value(response.reason),
            "payload": payload_value,
            "raw_body_preview": raw_body_preview,
        },
    )


def _safe_json_payload(response: requests.Response) -> Any | None:
    try:
        return response.json()
    except ValueError:
        return None


def _coerce_success_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _looks_like_offline_result(payload: dict[str, Any]) -> bool:
    return any(
        key in payload
        for key in ("success", "returncode", "offline_run_errors", "offline_run_log", "stdout")
    )


def _build_upstream_failure_message(payload: dict[str, Any]) -> str:
    errors = payload.get("offline_run_errors")
    if isinstance(errors, list):
        offline_run_errors_text = "; ".join(str(item) for item in errors if item is not None)
    elif errors is None:
        offline_run_errors_text = ""
    else:
        offline_run_errors_text = str(errors)

    error_text = offline_run_errors_text

    message_value = payload.get("message")
    message_text = str(message_value).strip() if message_value is not None else ""

    stderr_value = payload.get("stderr")
    stderr_text = str(stderr_value).strip() if stderr_value is not None else ""

    offline_run_log_value = payload.get("offline_run_log")
    offline_run_log_text = (
        str(offline_run_log_value).strip() if offline_run_log_value is not None else ""
    )

    if message_text:
        message_text = truncate_with_marker(message_text, "message")
    if stderr_text:
        stderr_text = truncate_with_marker(stderr_text, "stderr")
    if offline_run_errors_text:
        offline_run_errors_text = truncate_with_marker(
            offline_run_errors_text, "offline_run_errors"
        )
    if offline_run_log_text:
        offline_run_log_text = truncate_with_marker(
            offline_run_log_text, "offline_run_log"
        )

    # Use the most specific upstream signal as the primary error summary.
    if not error_text:
        if message_text:
            error_text = message_text
        elif stderr_text:
            error_text = stderr_text

    returncode = payload.get("returncode")
    base_message = f"Offline override failed upstream (returncode={returncode})."
    if error_text:
        base_message = f"{base_message} {error_text}"

    detail_lines: list[str] = []
    if message_text:
        detail_lines.append(f"message={message_text}")
    if offline_run_errors_text:
        detail_lines.append(f"offline_run_errors={offline_run_errors_text}")
    if stderr_text:
        detail_lines.append(f"stderr={stderr_text}")
    if offline_run_log_text:
        detail_lines.append(f"offline_run_log={offline_run_log_text}")

    if detail_lines:
        return f"{base_message}\n" + "\n".join(detail_lines)
    return base_message


def _request_certs() -> Tuple[str, str]:
    cert_path = os.environ.get("CERT_PATH")
    key_path = os.environ.get("KEY_PATH")
    if not cert_path or not key_path:
        raise ValueError("CERT_PATH and KEY_PATH environment variables are required")
    return cert_path, key_path


def fetch_override_and_token() -> tuple[str, str]:
    """Fetch the current override body and optimistic concurrency token."""
    override_token_url = os.environ.get("OVERRIDE_TOKEN_URL")
    if not override_token_url:
        raise ValueError("OVERRIDE_TOKEN_URL environment variable is required")

    cert = _request_certs()
    _log_api_request("GET", override_token_url)

    try:
        response = requests.get(override_token_url, cert=cert, timeout=300, verify=False)
        _log_api_response(response, payload=_safe_json_payload(response))
        response.raise_for_status()

        payload = response.json()
        stdout = payload.get("stdout", "")
        if not isinstance(stdout, str):
            raise TypeError("override token response field 'stdout' must be a string")

        parsed_stdout = ast.literal_eval(stdout.strip())
        if not isinstance(parsed_stdout, dict):
            raise TypeError("override token stdout must parse to a dictionary")

        token = parsed_stdout.get("token")
        override_body = parsed_stdout.get("override")
        if not isinstance(token, str) or not isinstance(override_body, str):
            raise TypeError("override token stdout must contain string 'token' and 'override' fields")

        logger.info(
            "Parsed override token payload",
            extra={
                "token": redact_value(token),
                "override_length": len(override_body),
            },
        )
        return token, override_body

    except (requests.RequestException, ValueError, TypeError, SyntaxError) as exc:
        logger.exception(
            "Failed to fetch override token payload from override endpoint",
            extra={"error": redact_value(str(exc))},
        )
        raise


def submit_offline_override(updated_toml: str, base_token: str) -> dict[str, Any]:
    """Submit an updated override TOML along with base token for concurrency validation."""
    run_offline_override_url = os.environ.get("RUN_OFFLINE_OVERRIDE_URL")
    if not run_offline_override_url:
        raise ValueError("RUN_OFFLINE_OVERRIDE_URL environment variable is required")

    cert = _request_certs()
    raw_timeout = os.environ.get("RUN_OFFLINE_OVERRIDE_TIMEOUT_SEC")
    timeout_seconds = DEFAULT_HTTP_TIMEOUT_SECONDS
    if raw_timeout is not None:
        try:
            parsed_timeout = float(raw_timeout)
            if parsed_timeout > 0:
                timeout_seconds = parsed_timeout
        except ValueError:
            pass
    request_payload = {
        "updated_toml": updated_toml,
        "base_token": base_token,
    }
    _log_api_request("POST", run_offline_override_url, payload=request_payload)

    try:
        response = requests.post(
            run_offline_override_url,
            json=request_payload,
            cert=cert,
            timeout=timeout_seconds,
            verify=False
        )
        _log_api_response(response, payload=_safe_json_payload(response))
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError("offline override response payload must be a dictionary")

        stdout_value = payload.get("stdout")
        parsed_stdout: dict[str, Any]

        if isinstance(stdout_value, str) and stdout_value.strip():
            try:
                maybe_parsed = ast.literal_eval(stdout_value.strip())
            except (ValueError, SyntaxError) as exc:
                if _looks_like_offline_result(payload):
                    raise ValueError(_build_upstream_failure_message(payload)) from exc
                raise
            if not isinstance(maybe_parsed, dict):
                raise TypeError("offline override stdout must parse to a dictionary")
            parsed_stdout = maybe_parsed
        elif _looks_like_offline_result(payload):
            # Some endpoints return the final structured result directly (without stdout wrapping).
            parsed_stdout = payload
        else:
            raise TypeError("offline override response is missing parseable stdout payload")

        if not _coerce_success_flag(parsed_stdout.get("success", True)):
            raise ValueError(_build_upstream_failure_message(parsed_stdout))

        logger.info(
            "Parsed offline override response payload",
            extra={"parsed_stdout": redact_value(parsed_stdout)},
        )
        return parsed_stdout

    except (requests.RequestException, ValueError, TypeError, SyntaxError) as exc:
        logger.exception(
            "Failed to submit offline override payload to endpoint",
            extra={"error": redact_value(str(exc))},
        )
        raise


def _normalize_diff_payload(payload: Any) -> list[dict[str, Any]]:
    """Coerce the diff endpoint response into a list of per-file diff entries."""
    # Some endpoints wrap the real result in a `stdout` string.
    if isinstance(payload, dict) and "filename" not in payload:
        returncode = payload.get("returncode")
        if returncode not in (None, 0):
            raise ValueError(_build_upstream_failure_message(payload))

        stdout_value = payload.get("stdout")
        if isinstance(stdout_value, str) and stdout_value.strip():
            text = stdout_value.strip()
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                try:
                    payload = ast.literal_eval(text)
                except (ValueError, SyntaxError) as exc:
                    raise ValueError(
                        "Offline/production diff stdout is not parseable JSON or Python "
                        "literal: " + truncate_with_marker(text, "stdout")
                    ) from exc
        elif "diffs" in payload or "files" in payload:
            payload = payload.get("diffs") or payload.get("files")

    # The parsed stdout may itself wrap the entries under a "diffs"/"files" key.
    if isinstance(payload, dict) and "filename" not in payload:
        nested = payload.get("diffs") or payload.get("files")
        if isinstance(nested, dict):
            payload = nested

    if isinstance(payload, dict):
        # Either a single file entry, or a mapping of filename -> entry.
        if "filename" in payload:
            return [payload]
        entries: list[dict[str, Any]] = []
        for filename, entry in payload.items():
            if isinstance(entry, dict):
                entries.append({"filename": filename, **entry})
        if entries:
            return entries
        raise TypeError("Offline/production diff payload contained no file entries")

    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, dict)]

    raise TypeError("Offline/production diff payload must be a list or dictionary")


def fetch_offline_prod_csv_diff() -> list[dict[str, Any]]:
    """Fetch per-file offline-vs-production CSV diffs from the diff endpoint.

    Each returned entry contains ``filename``, ``offline_last_modified``,
    ``production_last_modified``, ``structure_diffs`` and ``quota_diffs``.
    """
    diff_url = os.environ.get("OFFLINE_PROD_DIFF_ERRORS_URL")
    if not diff_url:
        raise ValueError("OFFLINE_PROD_DIFF_ERRORS_URL environment variable is required")

    cert = _request_certs()
    _log_api_request("GET", diff_url)

    try:
        response = requests.get(diff_url, cert=cert, timeout=300, verify=False)
        _log_api_response(response, payload=_safe_json_payload(response))
        response.raise_for_status()

        entries = _normalize_diff_payload(response.json())
        logger.info(
            "Parsed offline/production CSV diff payload",
            extra={"file_count": len(entries)},
        )
        return entries

    except (requests.RequestException, ValueError, TypeError, SyntaxError) as exc:
        logger.exception(
            "Failed to fetch offline/production CSV diff payload",
            extra={"error": redact_value(str(exc))},
        )
        raise

def deploy_override(
    updated_toml: str,
    git_leroy_output_token: str,
    override_token: str | None = None,
    base_token: str | None = None,
) -> dict[str, Any]:
    """Promote a previously computed offline override run to production.

    Call only after the offline computation succeeded and the user approved the
    offline-vs-production diff.
    """
    deploy_url = os.environ.get("RUN_DEPLOY_OFFLINE_OVERRIDE")
    if not deploy_url:
        raise ValueError("RUN_DEPLOY_OFFLINE_OVERRIDE environment variable is required")

    if not updated_toml or not updated_toml.strip():
        raise ValueError("updated_toml is required to deploy an override")
    if not git_leroy_output_token:
        raise ValueError("git_leroy_output_token is required to deploy an override")

    cert = _request_certs()

    raw_timeout = os.environ.get("RUN_DEPLOY_OFFLINE_OVERRIDE_TIMEOUT_SEC")
    timeout_seconds = DEFAULT_HTTP_TIMEOUT_SECONDS
    if raw_timeout is not None:
        try:
            timeout_seconds = int(raw_timeout)
        except ValueError:
            logger.warning(
                "Invalid RUN_DEPLOY_OFFLINE_OVERRIDE_TIMEOUT_SEC=%r; using default %s",
                raw_timeout,
                DEFAULT_HTTP_TIMEOUT_SECONDS,
            )

    request_payload: dict[str, Any] = {
        "updated_toml": updated_toml,
        "git_leroy_output_token": git_leroy_output_token,
    }
    if override_token:
        request_payload["offline_override_token"] = override_token
    if base_token:
        # Production override revision the edit was based on, so the server can
        # reject the promote if production changed during review.
        request_payload["base_override_token"] = base_token

    _log_api_request("POST", deploy_url, payload=request_payload)

    try:
        response = requests.post(
            deploy_url,
            json=request_payload,
            cert=cert,
            timeout=timeout_seconds,
            verify=False,
        )
        _log_api_response(response, payload=_safe_json_payload(response))
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError("deploy override response payload must be a dictionary")

        if _looks_like_offline_result(payload) and not _coerce_success_flag(
            payload.get("success")
        ):
            raise RuntimeError(_build_upstream_failure_message(payload))

        return payload
    except (requests.RequestException, ValueError, TypeError) as exc:
        logger.exception("Failed to deploy override via deploy endpoint")
        raise RuntimeError(f"Deploy override request failed: {exc}") from exc