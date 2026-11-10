"""
Read helper for fetching data from Netarch.

Mirrors the write-side pattern in `push_manual_to_official.py` (which uses
`NetarchConnectionWrite` + `df.to_sql(...)`), but for reads: uses the
read-only `NetarchConnection` + `pandas.read_sql(...)`.
"""
from __future__ import annotations
from sqlalchemy import text
import logging
from typing import Any, Sequence

import pandas as pd

from netarch.netarch import NetarchConnection

logger = logging.getLogger(__name__)


def fetch_table(
    table_name: str,
    database: str = "netopt",
    columns: Sequence[str] | None = None,
    where: str | None = None,
    params: dict[str, Any] | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Fetch rows from a Netarch table into a DataFrame (read-only).

    Args:
        table_name: Name of the table to query. Must be a trusted/internal
            value (interpolated directly into SQL) — never pass raw user input.
        database: Netarch database to connect to. Defaults to "netopt".
        columns: Optional list of column names to select. Defaults to `*`.
        where: Optional SQL WHERE clause body (without the `WHERE` keyword),
            e.g. "region = :region". Use `params` for values to avoid
            SQL injection.
        params: Optional bind parameters referenced in `where`
            (e.g. {"region": "50565"}).
        limit: Optional row limit.

    Returns:
        pd.DataFrame: Query results.

    Example:
        >>> fetch_table(
        ...     "maprule_quotas_and_expected_offloads",
        ...     columns=["region", "maprule", "quota_pct"],
        ...     where="region = :region",
        ...     params={"region": "50565"},
        ...     limit=100,
        ... )
    """
    col_clause = ", ".join(columns) if columns else "*"
    query = f"SELECT {col_clause} FROM {table_name}"
    if where:
        query += f" WHERE {where}"
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    logger.info("Fetching from Netarch: database=%s table=%s", database, table_name)

    nc = NetarchConnection(database=database)
    engine = nc.get_engine()
    try:
        df = pd.read_sql(query, engine, params=params)
    finally:
        engine.dispose()

    logger.info("Fetched %d row(s) from %s.%s", len(df), database, table_name)
    return df

def fetch_query(
    query: str,
    database: str = "netopt",
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Run an arbitrary read-only SQL query against Netarch.
    # ...existing code...
    """
    logger.info("Running Netarch query against database=%s", database)

    nc = NetarchConnection(database=database)
    engine = nc.get_engine()
    try:
        df = pd.read_sql(text(query), engine, params=params)
    finally:
        engine.dispose()

    logger.info("Query returned %d row(s)", len(df))
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Example manual smoke test:
    # df = fetch_table("maprule_quotas_and_expected_offloads", limit=5)
    # print(df)