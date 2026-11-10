import json
import logging
from pathlib import Path
import os
import requests

from openai_agent.openai_agent_client import chat_completion

from .state import DiffAgentState
from .utils import process_all_diffs

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
ANALYST_SYSTEM_PROMPT = (_PROMPTS_DIR / "diff_analyst_system_prompt.txt").read_text(
	encoding="utf-8"
).strip()
logger = logging.getLogger(__name__)


def ingest_and_filter_node(state: DiffAgentState) -> dict:
	def _ingestion_error(details: str, raw_response: dict | None = None) -> dict:
		return {
			"toml_diff": "",
			"raw_csv_diff_response": raw_response or {},
			"filtered_csv_changes": {},
			"promotion_token": "",
			"is_pipeline_healthy": False,
			"error_message": json.dumps(
				{
					"ok": False,
					"error_type": "DiffIngestionError",
					"details": details,
				}
			),
		}

	try:
		logger.info("Starting ingest_and_filter_node")
		composite_token = state.get("composite_token")
		if not composite_token:
			return _ingestion_error("Missing required composite_token in state")
		base_url = os.environ.get("OFFLINE_MANUAL_PROD_CSV_DIFF_URL")
		if not base_url:
			return _ingestion_error("Missing required environment variable: OFFLINE_MANUAL_PROD_CSV_DIFF_URL")

		# url = f"{base_url.rstrip('/')}/v1/offline_manual_prod_csv_diff"
		url = base_url  # Use the base URL directly, assuming it points to the correct endpoint
		cert_path = os.environ.get("CERT_PATH")
		key_path = os.environ.get("KEY_PATH")
		cert_arg = (cert_path, key_path) if cert_path and key_path else None

		try:
			logger.info("Calling diff API: %s", url)
			resp = requests.get(
				url,
				params={"token": composite_token},
				timeout=60,
				cert=cert_arg,
				verify=False  # Set to True in production if you have a valid SSL certificate
			)
			# logger.info("Raw API response:\n%s", resp)
			resp.raise_for_status()
			raw_response = resp.json()
			logger.info("Raw API response:\n%s", json.dumps(raw_response, indent=2))
		except (ValueError, TypeError) as exc:
			return _ingestion_error(f"Request failed for {url}: {exc}")

		if not isinstance(raw_response, dict):
			return _ingestion_error("Response payload must be a JSON object")

		if raw_response.get("returncode") != 0:
			api_msg = str(raw_response.get("message", "No message provided")).strip() or "No message provided"
			api_stderr = str(raw_response.get("stderr", "")).strip()
			logger.error(
				"Diff API returned non-zero returncode (message=%s, stderr=%s)",
				api_msg,
				api_stderr or "<empty>",
			)
			details = (
				f"Diff API failed (returncode={raw_response.get('returncode')}) "
				f"message: {api_msg}; stderr: {api_stderr or 'No stderr provided'}"
			)
			return _ingestion_error(details, raw_response)

		toml_diff = raw_response.get("override_diff", "")
		token = raw_response.get("token", "")
		diffs = raw_response.get("diff", {})
		if not isinstance(diffs, dict):
			return _ingestion_error("Response field 'diff' must be a JSON object", raw_response)

		filtered_changes = process_all_diffs(diffs)
		logger.info(
			"Ingestion complete (token_present=%s, diff_files=%d)",
			bool(token),
			len(filtered_changes),
		)

		return {
			"toml_diff": toml_diff,
			"raw_csv_diff_response": raw_response,
			"filtered_csv_changes": filtered_changes,
			"promotion_token": token,
			"is_pipeline_healthy": True,
			"error_message": None,
		}
	except requests.RequestException as e:
		response_text = ""
		if e.response is not None:
			response_text = e.response.text
		logger.error(
			"ingest_and_filter_node request failed: %s raw_response_text=%s",
			e,
			response_text or "<none>",
			exc_info=True,
		)
		details = f"Request failed: {e}"
		if response_text:
			details += f" | raw_response: {response_text}"
		return _ingestion_error(details)
	except Exception as e:
		logger.error("ingest_and_filter_node failed: %s", e, exc_info=True)
		return _ingestion_error(str(e))


def analyze_and_correlate_node(state: DiffAgentState) -> dict:
	if not state.get("is_pipeline_healthy"):
		logger.warning("Pipeline unhealthy; skipping LLM correlation")
		return {
			"final_report": f"⚠️ **Pipeline Validation Failed**\n\n```text\n{state.get('error_message')}\n```"
		}

	csv_changes = json.dumps(state["filtered_csv_changes"], indent=2)
	user_message = f"TOML DIFF:\n{state['toml_diff']}\n\nFILTERED CSV CHANGES:\n{csv_changes}"

	try:
		logger.info("Starting analyze_and_correlate_node")
		logger.debug("LLM user_message payload:\n%s", user_message)
		resp = chat_completion(
			messages=[
				{"role": "system", "content": ANALYST_SYSTEM_PROMPT},
				{"role": "user", "content": user_message},
			]
		)
		response_text = resp["choices"][0]["message"]["content"].strip()
		token = state.get("promotion_token", "")
		if token:
			response_text += f"\n\n---\n**To promote this output, run:**\n`/promote @<approver_name> {token}`"
		logger.info("LLM correlation complete (report_len=%d)", len(response_text))
		return {"final_report": response_text}
	except Exception as e:
		logger.error("analyze_and_correlate_node failed: %s", e, exc_info=True)
		return {"final_report": f"⚠️ **LLM Analysis Failed**\n\n{str(e)}"}
