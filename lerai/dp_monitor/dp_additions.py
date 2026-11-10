"""
DP monitoring additions: functions to fetch deployment-project related
data from Netarch (and other sources as they're added) for the DP tracker
monitoring feature.
"""

from __future__ import annotations

import logging

import pandas as pd
import sys, os
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))


from netarch.fetch_table import fetch_query
from sqlflask.fetch_table import fetch_query as sqlflask_fetch_query
from dp_monitor.dp_tracker_db import get_tracked_dp_keys, insert_dp_tracker_row
from datetime import datetime, timezone

from dp_monitor.dp_tracker_db import (
    get_tracked_dp_keys,
    insert_dp_tracker_row,
    get_canonical_hwtype_mappings,
    save_canonical_hwtype_mapping,
    get_tracker_rows,
    update_active_machines_count,
    get_recently_deployed_tracker_rows,
    get_recently_live_tracker_rows,
)

# ...existing code...
CANONICAL_HWTYPES = ["Caribou-L", "Caribou-P", "Caribou-J", "X7p", "X8"]

# Fast rule-based matches, tried before falling back to an LLM call.
# Add more keywords here as you discover new raw naming variants.
_HWTYPE_KEYWORD_RULES: list[tuple[str, str]] = [
    ("caribou", "caribou"),  # marker only; refined below by suffix letter
]


def _rule_based_canonical_match(raw_hwtype: str) -> str | None:
    """Cheap keyword-based match, tried before calling the LLM."""
    text = raw_hwtype.lower()

    if "caribou" in text:
        if re.search(r"\bl\b", text) or "-l" in text or " l" in text:
            return "Caribou-L"
        if re.search(r"\bp\b", text) or "-p" in text or " p" in text:
            return "Caribou-P"
        if re.search(r"\bj\b", text) or "-j" in text or " j" in text:
            return "Caribou-J"
        return None  # caribou but unclear suffix — fall back to LLM

    if "x7p" in text or "x7-p" in text or "x7 p" in text:
        return "X7p"
    if "x8" in text:
        return "X8"
    if "x7" in text and "p" not in text:
        return None  # ambiguous: plain X7 isn't one of the 5 canonical types

    return None

def normalize_hwtype(raw_hwtype: str) -> str | None:
    """
    Normalizes a raw hardware type string (from either mcm's hardwareType
    or the DP's SERVER_TYPE) into one of CANONICAL_HWTYPES, or None if it
    doesn't correspond to any of them. Result is cached in SQLite so each
    unique raw string only costs one LLM call (if rules don't match) ever.
    """
    mappings = get_canonical_hwtype_mappings()
    if raw_hwtype in mappings:
        return mappings[raw_hwtype]

    canonical = _rule_based_canonical_match(raw_hwtype)

    if canonical is None:
        # Fall back to LLM for anything the keyword rules couldn't resolve.
        options = "\n".join(f"- {c}" for c in CANONICAL_HWTYPES)
        messages = [
            {
                "role": "system",
                "content": (
                    "You classify Akamai hardware/server type strings into "
                    "one of a fixed set of canonical hardware families. "
                    "Respond with ONLY the exact canonical name from the "
                    "list below that the input corresponds to. If none "
                    "apply, respond with exactly NONE."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Hardware/server type string: \"{raw_hwtype}\"\n\n"
                    f"Canonical hardware families:\n{options}"
                ),
            },
        ]
        try:
            response = chat_completion(messages)
            content = response["choices"][0]["message"]["content"].strip()
            canonical = content if content in CANONICAL_HWTYPES else None
            if content.upper() != "NONE" and canonical is None:
                logger.warning(
                    "LLM returned %r for %r, not in canonical list; treating as no match",
                    content, raw_hwtype,
                )
        except Exception:
            logger.exception("LLM canonical hwtype classification failed for: %s", raw_hwtype)
            canonical = None

    logger.info("Normalized hwtype %r -> %r", raw_hwtype, canonical)
    save_canonical_hwtype_mapping(raw_hwtype, canonical)
    return canonical

# repo root (two levels up from dp_monitor/) so `openai_agent` is importable

from openai_agent.openai_agent_client import chat_completion

logger = logging.getLogger(__name__)

MACHINE_THRESHOLD = 128

def extract_region_name_from_summary(dp_summary: str) -> str | None:
    """
    Uses an LLM call to extract the Large Region name from a DP summary.

    Example: "FABRIC-PAR4: Scale Large Region FABRIC-PAR41n" -> "FABRIC-PAR41n"

    Returns the region name string, or None if it couldn't be extracted.
    """
    messages = [
        {
            "role": "system",
            "content": (
                "You extract the Large Region name from Akamai deployment "
                "project (DP) summaries. The region name is the identifier "
                "that follows the phrase 'Scale Large Region'. Respond with "
                "ONLY the region name, nothing else. If no region name can "
                "be found, respond with exactly NONE."
            ),
        },
        {
            "role": "user",
            "content": f'DP summary: "{dp_summary}"',
        },
    ]

    try:
        response = chat_completion(messages)
        content = response["choices"][0]["message"]["content"].strip()
    except Exception:
        logger.exception("LLM region-name extraction failed for summary: %s", dp_summary)
        return None

    if not content or content.upper() == "NONE":
        logger.warning("No region name extracted from summary: %s", dp_summary)
        return None
    return content


def get_large_region_numbers(database: str = "netopt") -> list[str]:
    """
    Fetches Large Region numbers as a list of clean string values
    (e.g. "50463" instead of 50463.0).
    """
    lr_df = get_large_regions(database=database)
    return lr_df["REGION_NUMBER"].dropna().astype(int).astype(str).tolist()


def get_machine_counts_by_region(region_numbers: list[str]) -> pd.DataFrame:
    """
    Fetches the current ghost-machine IPs for the given regions via query2
    (through SQLFlask) and aggregates them into per-(region, hwtype)
    machine counts.

    Args:
        region_numbers: List of region numbers as strings, e.g. ["50463", "50565"].

    Returns:
        pd.DataFrame with columns: Region, hwType, Active_machines_count.
    """
    if not region_numbers:
        raise ValueError("region_numbers must be a non-empty list")

    req_regions = ", ".join(str(int(r)) for r in region_numbers)
    query = f"""
        SELECT
            REPLACE(
                CAST(m.ip AS STRING),
                '::ffff:',
                ''
            ) AS IP,
            m.region Region,
            hardwareType hwType
        FROM
            mcm_machines_v2 m
        WHERE
            m.primaryRole = 'ghost'
            AND m.region IN ({req_regions})
    """

    logger.info("Fetching machine IPs for %d region(s) via query2", len(region_numbers))
    df = sqlflask_fetch_query(service=os.environ.get("QUERY2_URL"), query=query)
    logger.info("Fetched %d machine row(s)", len(df))

    counts_df = (
        df.groupby(["Region", "hwType"])
        .size()
        .reset_index(name="Active_machines_count")
    )
    return counts_df


def get_scale_large_region_additions(database: str = "netopt") -> pd.DataFrame:
    """
    Fetches "Scale Large Region" deployment projects (DPs) along with their
    combined server types, quantities, and region names (comma-concatenated).
    """
    query = """
        SELECT
            DP.PROJECT_NUMBER,
            DP.DP_SUMMARY,
            DP.STATUS,
            DP.POR_STATUS,
            DP.TARGET_DATE,
            GROUP_CONCAT(
                DISTINCT DPI.SERVER_TYPE
                ORDER BY
                    DPI.SERVER_TYPE SEPARATOR ', '
            ) AS COMBINED_SERVER_TYPES,
            GROUP_CONCAT(
                DPI.QUANTITY
                ORDER BY
                    DPI.SERVER_TYPE SEPARATOR ', '
            ) AS COMBINED_QUANTITIES,
            GROUP_CONCAT(
                DISTINCT DPI.REGION_NAME
                ORDER BY
                    DPI.REGION_NAME SEPARATOR ', '
            ) AS COMBINED_REGION_NAMES,
            DP.QA_ACTUAL
        FROM
            CMN_INT.AK_DEPLOYMENT_PROJECT DP
            LEFT OUTER JOIN CMN_INT.AK_DEPLOYMENT_PROJECT_IMPACT DPI ON DPI.DEPLOYMENT_PROJECT_ID = DP.DEPLOYMENT_PROJECT_ID,
            CMN_INT.AK_DATA_CENTER DC
        WHERE
            DP.DATA_CENTER_ID = DC.DATA_CENTER_ID
            AND DP.dp_summary like '%Scale Large Region%'
            AND DPI.ACTION = 'Remove'
            AND DP.STATUS NOT IN ('Canceled')
        GROUP BY
            DP.PROJECT_NUMBER,
            DP.DP_SUMMARY,
            DP.STATUS,
            DP.POR_STATUS,
            DP.TARGET_DATE,
            DP.QA_ACTUAL
        ORDER BY
            DP.STATUS,
            DP.POR_STATUS
    """

    logger.info("Fetching Scale Large Region removal DPs from Netarch")
    df = fetch_query(query, database=database)
    logger.info("Fetched %d Scale Large Region removal DP row(s)", len(df))
    return df


def get_scale_large_region_additions_detailed(database: str = "netopt") -> pd.DataFrame:
    """
    Like get_scale_large_region_additions, but returns one row per
    (DP, Region, HWType) instead of comma-concatenated lists, so quantities
    can be unambiguously matched to the region/hwtype they apply to.

    NOTE: filters DPI.ACTION = 'Add' (machine additions). Verify this is
    the correct ACTION value in AK_DEPLOYMENT_PROJECT_IMPACT.

    Returns:
        pd.DataFrame with columns: PROJECT_NUMBER, DP_SUMMARY, STATUS,
        POR_STATUS, TARGET_DATE, SERVER_TYPE, REGION_NAME, QUANTITY, QA_ACTUAL.
    """
    query = """
        SELECT
            DP.PROJECT_NUMBER,
            DP.DP_SUMMARY,
            DP.STATUS,
            DP.POR_STATUS,
            DP.TARGET_DATE,
            DPI.SERVER_TYPE,
            DPI.REGION_NAME,
            SUM(DPI.QUANTITY) AS QUANTITY,
            DP.QA_ACTUAL
        FROM
            CMN_INT.AK_DEPLOYMENT_PROJECT DP
            LEFT OUTER JOIN CMN_INT.AK_DEPLOYMENT_PROJECT_IMPACT DPI ON DPI.DEPLOYMENT_PROJECT_ID = DP.DEPLOYMENT_PROJECT_ID,
            CMN_INT.AK_DATA_CENTER DC
        WHERE
            DP.DATA_CENTER_ID = DC.DATA_CENTER_ID
            AND DP.dp_summary like '%Scale Large Region%'
            AND DPI.ACTION = 'Add'
            AND DP.STATUS NOT IN ('Canceled')
        GROUP BY
            DP.PROJECT_NUMBER,
            DP.DP_SUMMARY,
            DP.STATUS,
            DP.POR_STATUS,
            DP.TARGET_DATE,
            DPI.SERVER_TYPE,
            DPI.REGION_NAME,
            DP.QA_ACTUAL
        ORDER BY
            DP.STATUS,
            DP.POR_STATUS
    """

    logger.info("Fetching Scale Large Region addition DP/region/hwtype rows from Netarch")
    df = fetch_query(query, database=database)
    logger.info("Fetched %d addition row(s)", len(df))
    return df


def get_large_regions(database: str = "netopt") -> pd.DataFrame:
    """
    Fetches the list of Large Region (LR) region numbers currently live
    in the network.

    Returns:
        pd.DataFrame with column: REGION_NUMBER.
    """
    query = """
        SELECT
            REGION_NUMBER,
            REGION_NAME
        FROM
            CMN_INT.AK_REGION
        WHERE
            region_use_type LIKE '%lr%'
            AND status = 'Live'
    """

    logger.info("Fetching Large Region list from Netarch")
    df = fetch_query(query, database=database)
    logger.info("Fetched %d Large Region row(s)", len(df))
    return df

def check_and_notify_new_large_dps() -> list[dict]:
    """
    Finds DPs with STATUS == 'Deploy'. Extracts each DP's Large Region name
    from its summary (via LLM), maps it to a region number using live LR
    data, normalizes hardware types on both sides to one of the 5 canonical
    types, then for each region sums the quantities across ALL pending
    (not-yet-tracked) DPs and hwtypes targeting that region. Adds that to
    the current region-wide machine count and flags the region if the
    cumulative total exceeds MACHINE_THRESHOLD — even if no single DP or
    hwtype alone would cross it.
    """
    additions_df = get_scale_large_region_additions_detailed()

    deploy_df = additions_df[additions_df["STATUS"] == "Deploy"].copy()

    if deploy_df.empty:
        logger.info("No DPs with STATUS == 'Deploy' found")
        return []

    # Build region name -> region number mapping from live Large Regions.
    lr_df = get_large_regions()
    name_to_number = {
        str(name).strip().lower(): str(int(num))
        for name, num in zip(lr_df["REGION_NAME"], lr_df["REGION_NUMBER"])
        if pd.notna(name) and pd.notna(num)
    }

    # Extract each DP's region name from its summary via LLM, then map to
    # a region number. Cache per unique summary to avoid repeat LLM calls.
    summary_to_region_number: dict[str, str | None] = {}
    for summary in deploy_df["DP_SUMMARY"].dropna().unique():
        extracted = extract_region_name_from_summary(summary)
        logger.info("Extracted region name from summary %r: %r", summary, extracted)
        if extracted is None:
            summary_to_region_number[summary] = None
            continue
        region_number = name_to_number.get(extracted.strip().lower())
        if region_number is None:
            logger.warning(
                "Extracted region name %r not found among live Large Regions "
                "(summary: %s)", extracted, summary,
            )
        summary_to_region_number[summary] = region_number

    deploy_df["REGION_NUMBER"] = deploy_df["DP_SUMMARY"].map(summary_to_region_number)
    unmapped_region = deploy_df[deploy_df["REGION_NUMBER"].isna()]
    
    if not unmapped_region.empty:
        logger.warning(
            "Skipping %d DP row(s) with unmappable region names: %s",
            len(unmapped_region), unmapped_region["PROJECT_NUMBER"].unique().tolist(),
        )
    deploy_df = deploy_df.dropna(subset=["REGION_NUMBER"])
    
    if deploy_df.empty:
        logger.info("No DEPLOY DPs with mappable regions")
        return []

    # Normalize DP SERVER_TYPE -> canonical hwtype BEFORE building tracking
    # keys, since the tracker table stores canonical hwtype, not raw SERVER_TYPE.
    deploy_df["CanonicalHWType"] = deploy_df["SERVER_TYPE"].apply(normalize_hwtype)

    unmapped_hw = deploy_df[deploy_df["CanonicalHWType"].isna()]
    
    if not unmapped_hw.empty:
        logger.warning(
            "Skipping %d DP row(s) with unmappable server types: %s",
            len(unmapped_hw), unmapped_hw["SERVER_TYPE"].unique().tolist(),
        )
    deploy_df = deploy_df.dropna(subset=["CanonicalHWType"])
    
    if deploy_df.empty:
        logger.info("No DEPLOY DPs with a canonical hwtype match")
        return []

    # Drop combos already tracked — we've already notified about those.
    # Keys use region NUMBERS and CANONICAL hwtype, matching the tracker table.
    tracked_keys = get_tracked_dp_keys()

    deploy_df["key"] = list(
        zip(
            deploy_df["PROJECT_NUMBER"].astype(str),
            deploy_df["REGION_NUMBER"].astype(str),
            deploy_df["CanonicalHWType"].astype(str),
        )
    )

    untracked_df = deploy_df[~deploy_df["key"].isin(tracked_keys)].copy()
    untracked_df["REGION_NUMBER"] = untracked_df["REGION_NUMBER"].astype(str)

    if untracked_df.empty:
        logger.info("All pending DPs already tracked")
        return []

    region_numbers = untracked_df["REGION_NUMBER"].unique().tolist()
    current_counts_df = get_machine_counts_by_region(region_numbers)

    # Normalize mcm hardwareType -> canonical hwtype, drop unmapped rows.
    current_counts_df["CanonicalHWType"] = current_counts_df["hwType"].apply(normalize_hwtype)
    current_counts_df = current_counts_df.dropna(subset=["CanonicalHWType"])

    # Per-(region, canonical hwtype) counts, for per-hwtype detail in tracker rows.
    current_counts = (
        current_counts_df.groupby(["Region", "CanonicalHWType"])["Active_machines_count"]
        .sum()
        .to_dict()
    )
    current_counts = {(str(r), str(h)): int(v) for (r, h), v in current_counts.items()}

    # Region-wide totals: all canonical hwtypes combined.
    current_region_totals: dict[str, int] = {}
    for (region, _hw), count in current_counts.items():
        current_region_totals[region] = current_region_totals.get(region, 0) + int(count)

    # Sum pending quantities per region across ALL untracked DPs and all
    # canonical hwtypes — catches multiple DPs and/or multiple hwtypes that
    # together push the region over the threshold.
    pending_region_totals = (
        untracked_df.groupby("REGION_NUMBER")["QUANTITY"].sum().to_dict()
    )

    # Also compute pending quantities per (region, hwtype), for accurate
    # per-hwtype Expected_machines_count reporting.
    pending_hwtype_totals = (
        untracked_df.groupby(["REGION_NUMBER", "CanonicalHWType"])["QUANTITY"]
        .sum()
        .to_dict()
    )

    newly_tracked: list[dict] = []

    for region, pending_qty in pending_region_totals.items():
        region = str(region)
        current_total = current_region_totals.get(region, 0)
        expected_total = current_total + int(pending_qty)

        if expected_total <= MACHINE_THRESHOLD:
            continue  # region as a whole stays within the threshold

        logger.info(
            "Region %s exceeds threshold: current=%d pending=%d expected=%d",
            region, current_total, int(pending_qty), expected_total,
        )

        # Record every contributing DP row (per canonical hwtype) for this region.
        contributing = untracked_df[untracked_df["REGION_NUMBER"] == region]
        for _, row in contributing.iterrows():
            dp_name = str(row["PROJECT_NUMBER"])
            hw_type = str(row["CanonicalHWType"])
            hw_current = int(current_counts.get((region, hw_type), 0))
            hw_pending = int(pending_hwtype_totals.get((region, hw_type), 0))
            hw_expected = hw_current + hw_pending
            insert_dp_tracker_row(
                dp_name=dp_name,
                region=region,
                hw_type=hw_type,
                active_machines_count=hw_current,
                expected_machines_count=hw_expected,
            )
            newly_tracked.append({
                "DPName": dp_name,
                "Region": region,
                "HWType": hw_type,
                "Active_machines_count": hw_current,
                "Expected_machines_count": hw_expected,
                "Machines_added": hw_pending,
                "Current_region_total": current_total,
                "Expected_region_total": expected_total,
                "Flagged_today": True,
            })

    # Also surface anything flagged within the last few days, even if it
    # wasn't newly tracked in *this* run (e.g. a previous run already
    # caught it, but it's still recent enough to be worth re-notifying).
    already_keys = {
        (item["DPName"], item["Region"], item["HWType"]) for item in newly_tracked
    }
    for item in newly_tracked:
        item["Update_date"] = item.get("Update_date") or datetime.now(timezone.utc).isoformat()

    for row in get_recently_deployed_tracker_rows(days=3):
        key = (row["DPName"], row["Region"], row["HWType"])
        if key in already_keys:
            continue
        newly_tracked.append({
            "DPName": row["DPName"],
            "Region": row["Region"],
            "HWType": row["HWType"],
            "Active_machines_count": row["Active_machines_count"],
            "Expected_machines_count": row["Expected_machines_count"],
            "Machines_added": row["Expected_machines_count"] - row["Active_machines_count"],
            "Current_region_total": None,
            "Expected_region_total": None,
            "Update_date": row["Deploy_state_update_date"],
            "Flagged_today": False,
        })

    return newly_tracked

def _format_large_dp_item(item: dict) -> list[str]:
    update_date = item.get("Update_date") or "unknown date"
    update_date = str(update_date)[:10]
    machines_added = item.get("Machines_added")
    current_region_total = item.get("Current_region_total")
    expected_region_total = item.get("Expected_region_total")

    lines: list[str] = []
    added_str = f"{machines_added} " if machines_added is not None else ""
    lines.append(
        f"- DP **{item['DPName']}** is scheduled to deploy "
        f"**{added_str}{item['HWType']}** machine(s) into Region `{item['Region']}`."
    )
    lines.append(
        f"  - {item['HWType']} machine count in this region: currently "
        f"**{item['Active_machines_count']}**, will become "
        f"**{item['Expected_machines_count']}** after this DP deploys."
    )
    if current_region_total is not None and expected_region_total is not None:
        lines.append(
            f"  - Region `{item['Region']}` total machine count, across ALL "
            f"hardware types: currently **{current_region_total}**, will become "
            f"**{expected_region_total}** after this DP deploys."
        )
    lines.append(f"  - _(flagged: {update_date})_")
    return lines


def format_large_dp_notification(newly_tracked: list[dict]) -> str:
    """
    Formats the list of newly-tracked (DP, Region, HWType) rows — plus any
    rows flagged within the last 3 days — from check_and_notify_new_large_dps()
    into a Webex markdown message, with today's newly-flagged DPs shown
    separately from ones flagged on a previous day (within the last 3 days).
    Returns an empty string if there's nothing to report.
    """
    if not newly_tracked:
        return ""

    today_items = [i for i in newly_tracked if i.get("Flagged_today")]
    recent_items = [i for i in newly_tracked if not i.get("Flagged_today")]

    lines: list[str] = []

    if today_items:
        lines.append("⚠️ **New today — Large Region DPs exceeding 128 machines**\n")
        for item in today_items:
            lines.extend(_format_large_dp_item(item))

    if recent_items:
        if lines:
            lines.append("")
        lines.append("🕒 **Flagged in the last 3 days (not new today)**\n")
        for item in recent_items:
            lines.extend(_format_large_dp_item(item))

    return "\n".join(lines)


def get_live_machine_counts_by_region(region_numbers: list[str]) -> pd.DataFrame:
    """
    Fetches currently-present (live) machines per region from
    netopt.large_region_machine_suspension_info (via Netarch), and
    separately fetches hardware types from mcm_machines_v2 (via SQLFlask
    query2), then merges the two locally on IP since they live on
    different backends and can't be joined in a single SQL query.

    Returns:
        pd.DataFrame with columns: Region, hwType, Live_machines_count.
    """
    if not region_numbers:
        raise ValueError("region_numbers must be a non-empty list")

    req_regions = ", ".join(str(int(r)) for r in region_numbers)

    # 1. Fetch present/live machine IPs per region from Netarch.
    suspension_query = f"""
        SELECT
            IP,
            Region,
            Status
        FROM
            netopt.large_region_machine_suspension_info
        WHERE
            Status = 'present'
            AND Region IN ({req_regions})
    """
    logger.info(
        "Fetching live machine suspension info for %d region(s) via Netarch",
        len(region_numbers),
    )
    suspension_df = fetch_query(suspension_query, database="netopt")
    logger.info("Fetched %d live suspension row(s)", len(suspension_df))

    if suspension_df.empty:
        return pd.DataFrame(columns=["Region", "hwType", "Live_machines_count"])

    # 2. Fetch hardware types for all machines in these regions from
    #    mcm_machines_v2 via SQLFlask query2.
    mcm_query = f"""
        SELECT
            REPLACE(
                CAST(m.ip AS STRING),
                '::ffff:',
                ''
            ) AS IP,
            m.region Region,
            hardwareType hwType
        FROM
            mcm_machines_v2 m
        WHERE
            m.primaryRole = 'ghost'
            AND m.region IN ({req_regions})
    """
    logger.info(
        "Fetching machine hwtypes for %d region(s) via query2", len(region_numbers)
    )
    mcm_df = sqlflask_fetch_query(service=os.environ.get("QUERY2_URL"), query=mcm_query)
    logger.info("Fetched %d mcm machine row(s)", len(mcm_df))

    if mcm_df.empty:
        return pd.DataFrame(columns=["Region", "hwType", "Live_machines_count"])

    # 3. Merge locally on IP. Normalize both IP columns (strip whitespace,
    #    lowercase) to reduce the chance of a silent merge mismatch.
    suspension_df = suspension_df.copy()
    mcm_df = mcm_df.copy()
    suspension_df["IP"] = suspension_df["IP"].astype(str).str.strip().str.lower()
    mcm_df["IP"] = mcm_df["IP"].astype(str).str.strip().str.lower()

    merged_df = suspension_df.merge(
        mcm_df[["IP", "hwType"]], on="IP", how="inner"
    )
    logger.info(
        "Merged %d/%d live suspension row(s) with a matching mcm hwtype",
        len(merged_df), len(suspension_df),
    )

    counts_df = (
        merged_df.groupby(["Region", "hwType"])
        .size()
        .reset_index(name="Live_machines_count")
    )
    return counts_df

def check_and_update_live_machine_counts() -> list[dict]:
    """
    For every row in dp_tracker_lr_gt_128_machines, checks the current live
    (present) machine count for that (Region, HWType) via
    netopt.large_region_machine_suspension_info. If the live count has
    increased beyond what's currently stored in Active_machines_count, the
    row is updated (Active_machines_count + Live_machines_update_date) and
    included in the returned notification list.

    Returns:
        List of dicts describing each updated tracker row, suitable for
        building a notification message.
    """
    tracker_rows = get_tracker_rows()
    if not tracker_rows:
        logger.info("No tracker rows to check for live machine updates")
        return []

    regions = sorted({row["Region"] for row in tracker_rows})
    live_counts_df = get_live_machine_counts_by_region(regions)

    # Normalize hwType -> canonical hwtype, same as the pending-DP pipeline.
    live_counts_df["CanonicalHWType"] = live_counts_df["hwType"].apply(normalize_hwtype)
    live_counts_df = live_counts_df.dropna(subset=["CanonicalHWType"])

    live_counts = (
        live_counts_df.groupby(["Region", "CanonicalHWType"])["Live_machines_count"]
        .sum()
        .to_dict()
    )
    live_counts = {(str(r), str(h)): int(v) for (r, h), v in live_counts.items()}

    print(f"DEBUG: live_counts = {live_counts}")

    updated: list[dict] = []

    for row in tracker_rows:
        key = (str(row["Region"]), str(row["HWType"]))
        new_live_count = live_counts.get(key)

        if new_live_count is None:
            continue  # no live data for this (region, hwtype) combo

        current_stored = int(row["Active_machines_count"])
        if new_live_count <= current_stored:
            continue  # no new machines seen live yet

        logger.info(
            "Live machine count increased for DP=%s region=%s hwType=%s: "
            "%d -> %d",
            row["DPName"], row["Region"], row["HWType"], current_stored, new_live_count,
        )
        update_active_machines_count(
            dp_name=row["DPName"],
            region=row["Region"],
            hw_type=row["HWType"],
            new_active_count=new_live_count,
        )
        updated.append({
            "DPName": row["DPName"],
            "Region": row["Region"],
            "HWType": row["HWType"],
            "Previous_active_machines_count": current_stored,
            "New_active_machines_count": new_live_count,
            "Expected_machines_count": row["Expected_machines_count"],
            "Remaining_to_deploy": max(row["Expected_machines_count"] - new_live_count, 0),
            "Updated_today": True,
        })

    already_keys = {
        (item["DPName"], item["Region"], item["HWType"]) for item in updated
    }
    for item in updated:
        item["Update_date"] = item.get("Update_date") or datetime.now(timezone.utc).isoformat()

    for row in get_recently_live_tracker_rows(days=3):
        key = (row["DPName"], row["Region"], row["HWType"])
        if key in already_keys:
            continue
        updated.append({
            "DPName": row["DPName"],
            "Region": row["Region"],
            "HWType": row["HWType"],
            "Previous_active_machines_count": None,
            "New_active_machines_count": row["Active_machines_count"],
            "Expected_machines_count": row["Expected_machines_count"],
            "Remaining_to_deploy": max(row["Expected_machines_count"] - row["Active_machines_count"], 0),
            "Update_date": row["Live_machines_update_date"],
            "Updated_today": False,
        })

    return updated

def _format_live_update_item(item: dict) -> list[str]:
    update_date = item.get("Update_date") or "unknown date"
    update_date = str(update_date)[:10]
    prev = item["Previous_active_machines_count"]
    prev_str = str(prev) if prev is not None else "unknown"
    remaining = item.get("Remaining_to_deploy")

    lines: list[str] = []
    lines.append(
        f"- DP **{item['DPName']}** — Region `{item['Region']}` ({item['HWType']}):"
    )
    lines.append(
        f"  - Previously seen: {prev_str} live machine(s) → "
        f"Now seeing: **{item['New_active_machines_count']}** live machine(s)"
    )
    lines.append(
        f"  - Total machines expected once this DP fully deploys: "
        f"**{item['Expected_machines_count']}**"
    )
    if remaining is not None:
        if remaining > 0:
            lines.append(f"  - Still remaining to go live: **{remaining}**")
        else:
            lines.append("  - ✅ Fully live — all expected machines are now present")
    lines.append(f"  - _(updated: {update_date})_")
    return lines


def format_live_machine_update_notification(updated: list[dict]) -> str:
    """
    Formats the list of updated tracker rows — plus any rows updated
    within the last 3 days — from check_and_update_live_machine_counts()
    into a Webex markdown message, with today's updates shown separately
    from ones that were updated on a previous day (within the last 3 days).
    """
    if not updated:
        return ""

    today_items = [i for i in updated if i.get("Updated_today")]
    recent_items = [i for i in updated if not i.get("Updated_today")]

    lines: list[str] = []

    if today_items:
        lines.append("✅ **New today — Large Region DP machines going live**\n")
        for item in today_items:
            lines.extend(_format_live_update_item(item))

    if recent_items:
        if lines:
            lines.append("")
        lines.append("🕒 **Updated in the last 3 days (not new today)**\n")
        for item in recent_items:
            lines.extend(_format_live_update_item(item))

    return "\n".join(lines)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    #new_dps = check_and_notify_new_large_dps()
    #print(new_dps)
    check_and_update_live_machine_counts()