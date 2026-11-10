"""
One-time (idempotent) setup script for the DP tracker database.

Creates `lerai.db` in the current working directory (or a path passed on the
command line) with a `dp_tracker_lr_gt_128_machines` table used to track DPs
with more than 128 machines.

Usage:
    python init_dp_tracker_db.py [path/to/lerai.db]
"""
from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = "lerai.db"

TABLE_NAME = "dp_tracker_lr_gt_128_machines"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    DPName TEXT NOT NULL,
    Region TEXT NOT NULL,
    HWType TEXT NOT NULL,
    Active_machines_count INTEGER,
    Expected_machines_count INTEGER,
    Deploy_state_update_date TEXT,
    Live_machines_update_date TEXT,
    PRIMARY KEY (DPName, Region, HWType)
);
"""


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Create lerai.db (if missing) and ensure the DP tracker table exists."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Initializing database at %s", path)
    conn = sqlite3.connect(path)
    try:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("Table '%s' is ready in %s", TABLE_NAME, path)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    init_db(db_path)