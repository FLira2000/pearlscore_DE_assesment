#!/usr/bin/env python3
"""Load seed_data CSVs into MySQL `billing` (source for the dlt pipeline)."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from urllib.parse import urlparse

import pymysql

ROOT = Path(__file__).resolve().parents[1]
SEED_DIR = ROOT / "seed_data"

MYSQL_URL = os.getenv(
    "MYSQL_URL",
    "mysql+pymysql://billing_user:billing_password@127.0.0.1:3306/billing",
)

TABLES = {
    "raw_customers": SEED_DIR / "raw_customers.csv",
    "raw_subscriptions": SEED_DIR / "raw_subscriptions.csv",
    "raw_invoices": SEED_DIR / "raw_invoices.csv",
}

SCHEMA_SQL = (ROOT / "mysql" / "init" / "01_schema.sql").read_text()


def _connect(url: str) -> pymysql.Connection:
    # Accept SQLAlchemy-style URLs used elsewhere in the repo.
    parsed = urlparse(url.replace("mysql+pymysql://", "mysql://", 1))
    return pymysql.connect(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 3306,
        user=parsed.username or "billing_user",
        password=parsed.password or "billing_password",
        database=(parsed.path or "/billing").lstrip("/") or "billing",
        charset="utf8mb4",
        autocommit=False,
    )


def _blank_to_none(row: dict[str, str]) -> dict[str, str | None]:
    return {k: (None if v == "" else v) for k, v in row.items()}


def main() -> None:
    conn = _connect(MYSQL_URL)
    try:
        with conn.cursor() as cur:
            for stmt in SCHEMA_SQL.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)

            for table, path in TABLES.items():
                if not path.exists():
                    raise FileNotFoundError(path)

                cur.execute(f"DELETE FROM {table}")
                with path.open(newline="", encoding="utf-8") as fh:
                    reader = csv.DictReader(fh)
                    rows = [_blank_to_none(row) for row in reader]
                    if not rows:
                        print(f"seeded {table}: 0 rows from {path.name}")
                        continue
                    cols = list(rows[0].keys())
                    placeholders = ", ".join(["%s"] * len(cols))
                    col_list = ", ".join(f"`{c}`" for c in cols)
                    sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})"
                    cur.executemany(sql, [tuple(r[c] for c in cols) for r in rows])

                cur.execute(f"SELECT COUNT(*) FROM {table}")
                n = cur.fetchone()[0]
                print(f"seeded {table}: {n} rows from {path.name}")

        conn.commit()
        print("MySQL seed complete.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
