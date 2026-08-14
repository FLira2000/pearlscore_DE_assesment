#!/usr/bin/env python3
"""Wait until MySQL billing and Postgres analytics accept connections."""

from __future__ import annotations

import os
import sys
import time

import pymysql
from sqlalchemy.engine import make_url

POSTGRES_URL = os.getenv(
    "DESTINATION__POSTGRES__CREDENTIALS",
    "postgresql://dbt_user:dbt_password@127.0.0.1:5432/analytics",
)
MYSQL_URL = os.getenv(
    "MYSQL_URL",
    os.getenv(
        "SOURCES__SQL_DATABASE__CREDENTIALS",
        "mysql+pymysql://billing_user:billing_password@127.0.0.1:3306/billing",
    ),
)


def wait_postgres(attempts: int = 40, delay: float = 2.0) -> None:
    import psycopg2

    last: Exception | None = None
    for i in range(attempts):
        try:
            psycopg2.connect(POSTGRES_URL).close()
            print("postgres ready")
            return
        except Exception as exc:  # noqa: BLE001 — retry until timeout
            last = exc
            print(f"postgres not ready ({i + 1}/{attempts}): {exc}")
            time.sleep(delay)
    raise SystemExit(f"postgres did not become ready: {last}")


def wait_mysql(attempts: int = 40, delay: float = 2.0) -> None:
    url = make_url(MYSQL_URL)
    last: Exception | None = None
    for i in range(attempts):
        try:
            conn = pymysql.connect(
                host=url.host,
                port=url.port or 3306,
                user=url.username,
                password=url.password or "",
                database=url.database,
                connect_timeout=5,
            )
            conn.close()
            print("mysql ready")
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            print(f"mysql not ready ({i + 1}/{attempts}): {exc}")
            time.sleep(delay)
    raise SystemExit(f"mysql did not become ready: {last}")


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target in ("postgres", "all"):
        wait_postgres()
    if target in ("mysql", "all"):
        wait_mysql()


if __name__ == "__main__":
    main()
