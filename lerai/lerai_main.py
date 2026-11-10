# lerai/lerai_main.py

import os, threading, asyncio
import re
from pathlib import Path
import logging
from webex_bot.webex_bot import WebexBot
from webexteamssdk import WebexTeamsAPI
from apscheduler.schedulers.background import BackgroundScheduler
from scheduled_jobs import send_daily_offload_report, send_daily_csv_diff_report, send_daily_query2_variance_report, send_daily_quota_exceed_report

# Import commands
from lerai_commands import (
    CompareCsvEnvsCommand,
    AirflowErrorSummaryCommand,
    ExpectedObservedDiffCommand,
    LRDPCommand,
    #LRDPDevCommand,
    footprintCommand,
    QueryVarianceCommand,
    QuotaExceedCommand,
    #SimulateDailyReport,
    #SimulateDailyOffloadReport,
    # v1.6 promotion commands  
    PromoteCommand,
    ApproveCommand,
    LeroyOverrideWriterCommand,
    TriggerCplexCommand,
    DiffAnalystCommand,
    DiffAnalystV2Command,
    OfflineProdDiffCommand,
    LRPendingDPCommand,
    LRLiveMachinesCommand,
)

# Import scheduled jobs
from scheduled_jobs import (
    send_daily_csv_diff_report,
    send_daily_offload_report,
    send_daily_large_dp_report,
    send_daily_live_machine_report,
)
from lerai.logging_utils import configure_default_logging


logger = logging.getLogger(__name__)


class MentionOnlyWebexBot(WebexBot):
    """Process group-space messages only when the bot is directly mentioned."""

    def _is_directly_mentioned(self, raw_message: str) -> bool:
        if not raw_message:
            return False

        text = raw_message.lower()
        display_name = (self.bot_display_name or "").strip().lower()
        email = (self.bot_email or "").strip().lower()

        # Mentioned bot names usually appear as plain text in teams_message.text.
        if display_name and re.search(rf"\b{re.escape(display_name)}\b", text):
            return True

        # Fallback for formats that include the email in mention payload text.
        if email and email in text:
            return True

        return False

    def process_incoming_message(self, teams_message, activity):
        is_one_on_one_space = 'ONE_ON_ONE' in activity['target']['tags']
        raw_message = getattr(teams_message, "text", "") or ""

        # In group spaces, require explicit mention before invoking commands.
        if not is_one_on_one_space and not self._is_directly_mentioned(raw_message):
            return

        return super().process_incoming_message(teams_message, activity)

    def process_raw_command(self, raw_message, teams_message, user_email, activity, is_card_callback_command=False):
        if raw_message is None:
            raw_message = ""

        normalized = raw_message.strip().lower()
        if normalized == "/help":
            raw_message = "help"
            normalized = "help"

        command_found = False

        for c in self.commands:
            if not is_card_callback_command and c.command_keyword:
                command_keyword = c.command_keyword.strip().lower()
                if c.exact_command_keyword_match:
                    if normalized == command_keyword:
                        command_found = True
                        break
                else:
                    # Match either the bare command or command followed by arguments.
                    # Avoid substring collisions like "/analyze_diff" matching "/analyze_diff_v2".
                    if normalized == command_keyword or normalized.startswith(f"{command_keyword} "):
                        command_found = True
                        break
            else:
                if normalized == c.command_keyword or normalized == c.card_callback_keyword:
                    command_found = True
                    break

        # --- DYNAMIC THREAD ROUTING FIX ---
        # If no explicit command is found, but the message is part of an existing thread,
        # fetch the root message of the thread to determine which agent owns it.
        if not command_found and not is_card_callback_command:
            parent_id = None
            if isinstance(activity, dict) and "parent" in activity:
                parent_id = activity["parent"].get("id")
            elif hasattr(teams_message, "parentId") and teams_message.parentId:
                parent_id = teams_message.parentId

            if parent_id:
                target_command = "/write_override"  # Default fallback
                try:
                    token = os.environ.get("WEBEX_ACCESS_TOKEN")
                    if token:
                        from webexteamssdk import WebexTeamsAPI
                        api = WebexTeamsAPI(access_token=token)
                        root_msg = api.messages.get(parent_id)
                        root_text = (root_msg.text or "").lower()

                        # Identify the agent based on the root command
                        if "/trigger_cplex" in root_text:
                            target_command = "/trigger_cplex"
                        elif "/analyze_diff_v2" in root_text:
                            target_command = "/analyze_diff_v2"
                        elif "/analyze_diff" in root_text:
                            target_command = "/analyze_diff"
                        elif "diff_offline_prod" in root_text:
                            target_command = "diff_offline_prod"
                except Exception as e:
                    logger.warning(f"Failed to fetch thread root message for routing: {e}")

                # Implicitly prefix the command so the parent router handles it correctly
                raw_message = f"{target_command} {raw_message}"
                command_found = True
        # --- END DYNAMIC THREAD ROUTING FIX ---

        if not command_found:
            logger.info("Ignoring message with no matching command")
            return None

        return super().process_raw_command(raw_message, teams_message, user_email, activity, is_card_callback_command)


def run_bot(bot):
    logger.info("LeRAI v1.6 listening on async loop")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bot.run()
    logger.info("LeRAI async loop stopped")

def lerai_main(): 
    configure_default_logging()

    # ── Bot setup ─────────────────────────────────────────────────────────────
    BOT_TOKEN = os.environ.get("WEBEX_ACCESS_TOKEN", "default_value")
    SPACE_ID  = os.environ.get("WEBEX_SPACE_ID")

    bot = MentionOnlyWebexBot(
        teams_bot_token=BOT_TOKEN,
        bot_name="ShakaBot",
        include_demo_commands=False,
        approved_domains=['akamai.com']
    )

    # Commands
    bot.add_command(PromoteCommand())
    bot.add_command(ApproveCommand())
    bot.add_command(CompareCsvEnvsCommand())
    bot.add_command(AirflowErrorSummaryCommand())
    bot.add_command(ExpectedObservedDiffCommand())
    bot.add_command(LRDPCommand())
    #bot.add_command(LRDPDevCommand())
    bot.add_command(footprintCommand())
    bot.add_command(QueryVarianceCommand())
    bot.add_command(QuotaExceedCommand())
    bot.add_command(LeroyOverrideWriterCommand())
    bot.add_command(TriggerCplexCommand())
    bot.add_command(DiffAnalystV2Command())
    bot.add_command(DiffAnalystCommand())
    bot.add_command(OfflineProdDiffCommand())
    bot.add_command(LRPendingDPCommand())
    bot.add_command(LRLiveMachinesCommand())
    #bot.add_command(SimulateDailyReport())
    #bot.add_command(SimulateDailyOffloadReport())


    
    # --- Start the daily scheduler ---
    #scheduler = BackgroundScheduler()
    #scheduler.add_job(send_daily_offload_report, "cron", hour=14, minute=25)
    #scheduler.add_job(send_daily_csv_diff_report, "cron", hour=15, minute=20)
    #scheduler.add_job(send_daily_query2_variance_report, "cron", hour=6, minute=15)
    #scheduler.add_job(send_daily_quota_exceed_report, "cron", hour=19, minute=2)
    scheduler = BackgroundScheduler(timezone="America/New_York")
    scheduler.add_job(send_daily_large_dp_report, "cron", hour=11, minute=0)
    scheduler.add_job(send_daily_live_machine_report, "cron", hour=11, minute=0)
    scheduler.start()

    # What are my logging levels?
    # Iterate through all active loggers
    #for name in logging.root.manager.loggerDict:
    #    if "webex" in name:
    #        log = logging.getLogger(name)
    #        print(f"{name:<50} | Level: {logging.getLevelName(log.getEffectiveLevel())}")

    # List of all noisy prefixes found in your trace
    noisy_loggers = ['webex_bot', 'webex_websocket_client', 'webexpythonsdk', 'webexteamssdk']
    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.INFO)
        # This prevents the logs from bypassing settings via parent handlers
        logging.getLogger(logger_name).propagate = False
    
    # ── Start LeRAI thread ─────────────────────
    bot_thread = threading.Thread(target=run_bot, args=(bot,), daemon=True)
    bot_thread.start()

    logger.info("LeRAI v1.6 is running")
    bot_thread.join()

