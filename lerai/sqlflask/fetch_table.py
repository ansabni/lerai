"""
Read-helper utilities for fetching data via the SQLFlask AppBattery service.

Mirrors the style of netarch_fetch.py, but uses the HTTP-based SQLFlask
client (lerai.sqlflask.sqlflask.SQLFlask) instead of a direct SQLAlchemy
connection. SQLFlask proxies queries to common Akamai backend services
(query2, mako, mrom, netarch) over HTTPS with mTLS cert auth.
"""

from sqlflask.sqlflask import SQLFlask  # adjust import path as needed
import pandas as pd
import os

DEFAULT_CERT_PATH = os.environ.get("CERT_PATH")
DEFAULT_KEY_PATH = os.environ.get("KEY_PATH")
DEFAULT_SQLFLASK_URL = os.environ.get("SQL_FLASK_URL")  # placeholder, set real URL


def _get_client(cert_path: str = DEFAULT_CERT_PATH,
                key_path: str = DEFAULT_KEY_PATH,
                sqlflask_url: str = DEFAULT_SQLFLASK_URL) -> SQLFlask:
    client = SQLFlask()
    client.set_cert_paths(cert_path, key_path)
    client.set_sqlflask_url(sqlflask_url)
    return client


def fetch_query(service: str, query: str, **client_kwargs) -> pd.DataFrame:
    client = _get_client(**client_kwargs)
    return client.get_dataframe(service, query)


def fetch_table(service: str,
                 table_name: str,
                 columns: list[str] | None = None,
                 where: str | None = None,
                 limit: int | None = None,
                 **client_kwargs) -> pd.DataFrame:
    col_clause = ", ".join(columns) if columns else "*"
    query = f"SELECT {col_clause} FROM {table_name}"
    if where:
        query += f" WHERE {where}"
    if limit:
        query += f" LIMIT {limit}"
    return fetch_query(service, query, **client_kwargs)


def fetch_table_raw_json(service: str, query: str, **client_kwargs) -> dict:
    client = _get_client(**client_kwargs)
    if service not in client.available_services:
        raise ValueError(
            f"Unknown service '{service}'. Available services: {client.available_services}"
        )
    return client.run_query(service, query)


if __name__ == "__main__":
    df = fetch_table(
        service="netarch",
        table_name="some_table",
        limit=10,
        cert_path=DEFAULT_CERT_PATH,
        key_path=DEFAULT_KEY_PATH,
        sqlflask_url=DEFAULT_SQLFLASK_URL,
    )
    print(df)