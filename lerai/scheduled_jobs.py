import os
import time
import logging
from webexteamssdk import WebexTeamsAPI
from csv_env_diff import compare_offline_vs_production
from log_error_summary import get_airflow_error_summary
from expected_observed_comparison import run_offload_analysis_workflow
from query2_variance_addition import check_query2_for_variance_addition
from quota_exceed import check_query2_for_quota_exceed
from lerai.logging_utils import redact_value


logger = logging.getLogger(__name__)


# ── Bot setup ─────────────────────────────────────────────────────────────
webex_token = os.environ.get("WEBEX_ACCESS_TOKEN", "default_value")
space_id  = os.environ.get("WEBEX_SPACE_ID")
offload_space_id  = os.environ.get("LR_OFFLOAD_WEBEX_SPACE_ID")


def send_daily_csv_diff_report():
    """
    Scheduled job: runs once a day inside the bot process.
    - Checks if offline CSVs are stale (>36h).
    - If stale: posts a 'stale data' message.
    - If fresh: runs full CSV comparison.
    - In both cases: also collects and summarizes Airflow ERROR logs and appends to the message.
    """

    if not webex_token or not space_id:
        logger.warning("Daily CSV job skipped because WEBEX_ACCESS_TOKEN or WEBEX_SPACE_ID is not set")
        return

    api = WebexTeamsAPI(access_token=webex_token)

    # ---- Part 1: CSV diff (with staleness check) ----
    csv_section = ""
    try:
        csv_summary = compare_offline_vs_production(check_staleness_only=False,stale_hours=36)
        csv_section = (
            "✅ **CSV comparison**\n\n"
            "Daily offline vs production CSV comparison completed.\n\n"
            f"{csv_summary}\n"
        )
    except Exception as e:
        csv_section = (
            "❌ **CSV comparison**\n\n"
            f"Failed to run daily CSV comparison.\n\n"
            f"Error: `{e}`\n"
        )
        logger.exception("Daily CSV job error")

    # Avoid OpenAI rate limit, sleep for 2s
    time.sleep(2)  
    # ---- Part 2: Airflow log error summary ----
    try:
        airflow_summary = get_airflow_error_summary()
        airflow_section = (
            "🧩 **Airflow ERROR log summary (last 12 hours)**\n\n"
            f"{airflow_summary}\n"
        )
    except Exception as e:
        airflow_section = (
            "❌ **Airflow ERROR log summary**\n\n"
            f"Failed to collect or summarize Airflow logs.\n\n"
            f"Error: `{e}`\n"
        )
        logger.exception("Daily Airflow log job error")

    # ---- Combine sections and send one message ----
    full_message = (
        "📅 **Daily report**\n\n"
        f"{csv_section}\n"
        f"{airflow_section}"
    )

    api.messages.create(roomId=space_id, markdown=full_message)

def send_daily_offload_report():
    """
    Scheduled job: runs once a day inside the bot process.
    - just calls run_offload_analysis_workflow, which provides a GPT summary of expected/observed offload diff
    - If not null, sends it to the LR offload watch webex space
    """
    logger.info("Daily offload job starting", extra={"space_id": redact_value(offload_space_id)})
    if not webex_token or not offload_space_id:
        logger.warning("Daily offload job skipped because WEBEX_ACCESS_TOKEN or WEBEX_OFFLOAD_SPACE_ID is not set")
        return

    api = WebexTeamsAPI(access_token=webex_token)

    # ---- Part 1: Offload summary
    section = ""
    try:
        offload_summary = run_offload_analysis_workflow()
        section = (
                    "✅ **LR offload watch**\n\n"
                    "Daily expected-observed offload comparison completed.\n\n"
                    f"{offload_summary}"
                )
    except Exception as e:
        section = (
            "❌ **LR offload watch**\n\n"
            f"Failed to run daily expected-observed offload comparison.\n\n"
            f"Error: `{e}`\n"
        )
        logger.exception("Daily offload job error")

    api.messages.create(roomId=offload_space_id, markdown=section)

    
def send_daily_query2_variance_report():
    """
    Scheduled job: runs once a day inside the bot process.
    - just calls the query2 variance function, which provides a  summary of regions in need of variance
    - If not null, sends it to the LR OPs webex space
    """
    logger.info("Daily Query2 variance job starting")
    if not webex_token or not space_id:
        logger.warning("Daily Query2 variance job skipped because WEBEX_ACCESS_TOKEN or WEBEX_OFFLOAD_SPACE_ID is not set")
        return

    section = ""
    try:
        variance_summary = check_query2_for_variance_addition()
        if not variance_summary:
            return 
        section = (
                    "✅ **Query2 Variance Watch**\n\n"
                    f"{variance_summary}"
                )
    except Exception as e:
        section = (
            "❌ **Query2 Variance Watch**\n\n"
            f"Error: `{e}`\n"
        )
        logger.exception("Daily Query2 variance job error")

    api = WebexTeamsAPI(access_token=webex_token)
    api.messages.create(roomId=offload_space_id, markdown=section)

    
def send_daily_quota_exceed_report():
    """
    Scheduled job: runs once a day inside the bot process.
    - just calls the query2 variance function, which provides a summary of regions where quotas are exceeded
    - If not null, sends it to the LR OPs webex space
    """
    logger.info("Daily quota exceed job starting")
    if not webex_token or not space_id:
        logger.warning("Daily quota exceed job skipped because WEBEX_ACCESS_TOKEN or WEBEX_OFFLOAD_SPACE_ID is not set")
        return

    section = ""
    try:
        summary = check_query2_for_quota_exceed()
        if not summary:
            return 
        section = ( "📅 **Exceeding Quotas watch**\n\n"
                    f"{summary}"
                )
    except Exception as e:
        section = (
            "❌ **Exceeding Quotas watch**\n\n"
            f"Failed to run daily exceeding quotas check.\n\n"
            f"Error: `{e}`\n"
        )
        logger.exception("Daily exceeding quotas error")

    api = WebexTeamsAPI(access_token=webex_token)
    api.messages.create(roomId=offload_space_id, markdown=section)
    
# ...existing code...
from query2_variance_addition import check_query2_for_variance_addition
from quota_exceed import check_query2_for_quota_exceed
from lerai.logging_utils import redact_value
from dp_monitor.dp_additions import (
    check_and_notify_new_large_dps,
    format_large_dp_notification,
    check_and_update_live_machine_counts,
    format_live_machine_update_notification,
)
# ...existing code...

def send_daily_large_dp_report():
    """
    Scheduled job: runs once a day inside the bot process.
    - Calls check_and_notify_new_large_dps, which finds DPs in DEPLOY status
      whose Large Region will exceed 128 machines once the DP completes,
      and inserts corresponding rows into dp_tracker_lr_gt_128_machines.
    - If any new rows were tracked, posts a summary to the LR Ops webex space.
    """
    logger.info("Daily Large DP tracker job starting")
    if not webex_token or not space_id:
        logger.warning("Daily Large DP tracker job skipped because WEBEX_ACCESS_TOKEN or WEBEX_SPACE_ID is not set")
        return

    section = ""
    try:
        newly_tracked = check_and_notify_new_large_dps()
        message = format_large_dp_notification(newly_tracked)
        if not message:
            logger.info("No new large DPs found; nothing to report")
            return
        section = message
    except Exception as e:
        section = (
            "❌ **Large Region DP Watch**\n\n"
            f"Failed to run daily large DP check.\n\n"
            f"Error: `{e}`\n"
        )
        logger.exception("Daily Large DP tracker job error")

    api = WebexTeamsAPI(access_token=webex_token)
    api.messages.create(roomId=space_id, markdown=section)


def send_daily_live_machine_report():
    """
    Scheduled job: runs once a day inside the bot process.
    - Calls check_and_update_live_machine_counts, which checks whether
      machines for already-tracked (DP, Region, HWType) rows have gone
      live in the network, and updates dp_tracker_lr_gt_128_machines
      accordingly.
    - If any rows were updated (or were updated within the last 3 days),
      posts a summary to the LR Ops webex space.
    """
    logger.info("Daily Large DP live-machine job starting")
    if not webex_token or not space_id:
        logger.warning("Daily live-machine job skipped because WEBEX_ACCESS_TOKEN or WEBEX_SPACE_ID is not set")
        return

    section = ""
    try:
        updated = check_and_update_live_machine_counts()
        message = format_live_machine_update_notification(updated)
        if not message:
            logger.info("No live-machine updates found; nothing to report")
            return
        section = message
    except Exception as e:
        section = (
            "❌ **Large Region DP Live-Machine Watch**\n\n"
            f"Failed to run daily live-machine check.\n\n"
            f"Error: `{e}`\n"
        )
        logger.exception("Daily live-machine job error")

    api = WebexTeamsAPI(access_token=webex_token)
    api.messages.create(roomId=space_id, markdown=section)
