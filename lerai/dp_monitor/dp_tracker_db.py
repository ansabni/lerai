"""
Helper functions for reading/writing the dp_tracker_lr_gt_128_machines
table in databases/lerai.db.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import logging
logger = logging.getLogger(__name__)

# dp_tracker_db.py lives at lerai/lerai/dp_monitor/, databases/ lives at
# the repo root (lerai/databases/lerai.db). Adjust if your DB is elsewhere.
DB_PATH = Path(__file__).resolve().parents[2] / "databases" / "lerai.db"
TABLE_NAME = "dp_tracker_lr_gt_128_machines"


def _get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def get_tracked_dp_keys(db_path: Path = DB_PATH) -> set[tuple[str, str, str]]:
    """Returns the set of (DPName, Region, HWType) already tracked."""
    conn = _get_connection(db_path)
    try:
        rows = conn.execute(
            f"SELECT DPName, Region, HWType FROM {TABLE_NAME}"
        ).fetchall()
    finally:
        conn.close()
    return {(str(r[0]), str(r[1]), str(r[2])) for r in rows}


def insert_dp_tracker_row(
    dp_name: str,
    region: str,
    hw_type: str,
    active_machines_count: int,
    expected_machines_count: int,
    db_path: Path = DB_PATH,
) -> None:
    """Inserts one row into dp_tracker_lr_gt_128_machines."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_connection(db_path)
    try:
        conn.execute(
            f"""
            INSERT INTO {TABLE_NAME} (
                DPName, Region, HWType,
                Active_machines_count, Expected_machines_count,
                Deploy_state_update_date, Live_machines_update_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dp_name,
                region,
                hw_type,
                active_machines_count,
                expected_machines_count,
                now,
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


CANONICAL_MAPPING_TABLE = "hwtype_canonical_mapping"


def _ensure_canonical_mapping_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CANONICAL_MAPPING_TABLE} (
            raw_hwtype TEXT NOT NULL PRIMARY KEY,
            canonical_hwtype TEXT,   -- one of the 5 canonical types, or NULL
            updated_at TEXT NOT NULL
        )
        """
    )


def get_canonical_hwtype_mappings(db_path: Path = DB_PATH) -> dict[str, str | None]:
    """Returns all persisted raw_hwtype -> canonical_hwtype mappings."""
    conn = _get_connection(db_path)
    try:
        _ensure_canonical_mapping_table(conn)
        conn.commit()
        rows = conn.execute(
            f"SELECT raw_hwtype, canonical_hwtype FROM {CANONICAL_MAPPING_TABLE}"
        ).fetchall()
    finally:
        conn.close()
    return {str(r[0]): (str(r[1]) if r[1] is not None else None) for r in rows}


def save_canonical_hwtype_mapping(
    raw_hwtype: str, canonical_hwtype: str | None, db_path: Path = DB_PATH
) -> None:
    """Persists one raw_hwtype -> canonical_hwtype mapping (upsert)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_connection(db_path)
    try:
        _ensure_canonical_mapping_table(conn)
        conn.execute(
            f"""
            INSERT INTO {CANONICAL_MAPPING_TABLE} (raw_hwtype, canonical_hwtype, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(raw_hwtype) DO UPDATE SET
                canonical_hwtype = excluded.canonical_hwtype,
                updated_at = excluded.updated_at
            """,
            (raw_hwtype, canonical_hwtype, now),
        )
        conn.commit()
    finally:
        conn.close()

def get_tracker_rows(db_path: Path = DB_PATH) -> list[dict]:
    """
    Returns all rows from dp_tracker_lr_gt_128_machines as dicts, including
    rows that haven't gone live yet (Live_machines_update_date IS NULL).
    """
    conn = _get_connection(db_path)
    try:
        rows = conn.execute(
            f"""
            SELECT DPName, Region, HWType, Active_machines_count,
                   Expected_machines_count, Deploy_state_update_date,
                   Live_machines_update_date
            FROM {TABLE_NAME}
            """
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "DPName": r[0],
            "Region": r[1],
            "HWType": r[2],
            "Active_machines_count": r[3],
            "Expected_machines_count": r[4],
            "Deploy_state_update_date": r[5],
            "Live_machines_update_date": r[6],
        }
        for r in rows
    ]

def update_active_machines_count(
    dp_name: str,
    region: str,
    hw_type: str,
    new_active_count: int,
    db_path: Path = DB_PATH,
) -> None:
    """
    Updates Active_machines_count and stamps Live_machines_update_date for
    a specific tracked (DPName, Region, HWType) row.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_connection(db_path)
    try:
        conn.execute(
            f"""
            UPDATE {TABLE_NAME}
            SET Active_machines_count = ?,
                Live_machines_update_date = ?
            WHERE DPName = ? AND Region = ? AND HWType = ?
            """,
            (new_active_count, now, dp_name, region, hw_type),
        )
        conn.commit()
    finally:
        conn.close()

RECENT_UPDATE_WINDOW_DAYS = 3


def get_recently_deployed_tracker_rows(
    days: int = RECENT_UPDATE_WINDOW_DAYS, db_path: Path = DB_PATH
) -> list[dict]:
    """
    Returns tracker rows whose Deploy_state_update_date falls within the
    last `days` days — i.e. DPs that were (re-)flagged as pushing a region
    over the threshold recently, regardless of whether they were newly
    tracked in the current run.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn = _get_connection(db_path)
    try:
        rows = conn.execute(
            f"""
            SELECT DPName, Region, HWType, Active_machines_count,
                   Expected_machines_count, Deploy_state_update_date,
                   Live_machines_update_date
            FROM {TABLE_NAME}
            WHERE Deploy_state_update_date IS NOT NULL
              AND Deploy_state_update_date >= ?
            """,
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "DPName": r[0],
            "Region": r[1],
            "HWType": r[2],
            "Active_machines_count": r[3],
            "Expected_machines_count": r[4],
            "Deploy_state_update_date": r[5],
            "Live_machines_update_date": r[6],
        }
        for r in rows
    ]


def get_recently_live_tracker_rows(
    days: int = RECENT_UPDATE_WINDOW_DAYS, db_path: Path = DB_PATH
) -> list[dict]:
    """
    Returns tracker rows whose Live_machines_update_date falls within the
    last `days` days — i.e. DPs whose live machine count was bumped
    recently, regardless of whether they were updated in the current run.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn = _get_connection(db_path)
    try:
        rows = conn.execute(
            f"""
            SELECT DPName, Region, HWType, Active_machines_count,
                   Expected_machines_count, Deploy_state_update_date,
                   Live_machines_update_date
            FROM {TABLE_NAME}
            WHERE Live_machines_update_date IS NOT NULL
              AND Live_machines_update_date >= ?
            """,
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "DPName": r[0],
            "Region": r[1],
            "HWType": r[2],
            "Active_machines_count": r[3],
            "Expected_machines_count": r[4],
            "Deploy_state_update_date": r[5],
            "Live_machines_update_date": r[6],
        }
        for r in rows
    ]