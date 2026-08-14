#!/usr/bin/env python3
"""
dlt pipeline: MySQL billing OLTP → Postgres warehouse (CANDIDATE_BRIEF).

Extracts the three raw billing tables as-is (including planted quality issues)
and replaces them in Postgres `analytics.raw` for dbt.
"""

from __future__ import annotations

import os

import dlt
from dlt.destinations import postgres
from dlt.sources.sql_database import sql_table
from sqlalchemy import create_engine, text

# CANDIDATE_BRIEF warehouse
POSTGRES_URL = os.getenv(
    "DESTINATION__POSTGRES__CREDENTIALS",
    "postgresql://dbt_user:dbt_password@127.0.0.1:5432/analytics",
)
# Optional §0 source
MYSQL_URL = os.getenv(
    "SOURCES__SQL_DATABASE__CREDENTIALS",
    "mysql+pymysql://billing_user:billing_password@127.0.0.1:3306/billing",
)

MYSQL_SCHEMA = "billing"
POSTGRES_DATASET = "raw"

# Column order and types match seed_data CSVs / mysql/init/01_schema.sql
RAW_TABLES = {
    "raw_customers": {
        "columns": ["customer_id", "customer_name", "email", "country", "created_at"],
        "hints": {
            "customer_id": {"data_type": "text", "nullable": False},
            "customer_name": {"data_type": "text", "nullable": False},
            "email": {"data_type": "text", "nullable": False},
            "country": {"data_type": "text", "nullable": True},
            "created_at": {"data_type": "date", "nullable": False},
        },
    },
    "raw_subscriptions": {
        "columns": [
            "subscription_id",
            "customer_id",
            "plan_name",
            "monthly_price",
            "start_date",
            "end_date",
            "status",
        ],
        "hints": {
            "subscription_id": {"data_type": "text", "nullable": False},
            "customer_id": {"data_type": "text", "nullable": False},
            "plan_name": {"data_type": "text", "nullable": False},
            "monthly_price": {"data_type": "decimal", "precision": 12, "scale": 2, "nullable": False},
            "start_date": {"data_type": "date", "nullable": False},
            "end_date": {"data_type": "date", "nullable": True},
            "status": {"data_type": "text", "nullable": False},
        },
    },
    "raw_invoices": {
        "columns": [
            "invoice_id",
            "subscription_id",
            "invoice_date",
            "amount",
            "currency",
            "status",
        ],
        "hints": {
            "invoice_id": {"data_type": "text", "nullable": False},
            "subscription_id": {"data_type": "text", "nullable": False},
            "invoice_date": {"data_type": "date", "nullable": False},
            "amount": {"data_type": "decimal", "precision": 12, "scale": 2, "nullable": True},
            "currency": {"data_type": "text", "nullable": False},
            "status": {"data_type": "text", "nullable": False},
        },
    },
}


@dlt.source(name="billing_raw")
def billing_raw_source():
    """One dlt resource per raw table. No cleaning — dbt staging owns that."""
    for table_name, spec in RAW_TABLES.items():
        yield sql_table(
            credentials=MYSQL_URL,
            table=table_name,
            schema=MYSQL_SCHEMA,
            reflection_level="full_with_precision",
            included_columns=spec["columns"],
            chunk_size=1000,
            backend="sqlalchemy",
            write_disposition="replace",
        ).with_name(table_name).apply_hints(columns=spec["hints"])


def _count_mysql() -> dict[str, int]:
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    counts: dict[str, int] = {}
    with engine.connect() as conn:
        for table in RAW_TABLES:
            counts[table] = int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0)
    return counts


def _count_postgres() -> dict[str, int]:
    engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
    counts: dict[str, int] = {}
    with engine.connect() as conn:
        for table in RAW_TABLES:
            counts[table] = int(
                conn.execute(text(f"SELECT COUNT(*) FROM {POSTGRES_DATASET}.{table}")).scalar() or 0
            )
    return counts


def load_billing_to_postgres() -> None:
    source_counts = _count_mysql()
    print("MySQL source rows:", source_counts)

    pipeline = dlt.pipeline(
        pipeline_name="nordstack_billing",
        destination=postgres(credentials=POSTGRES_URL),
        dataset_name=POSTGRES_DATASET,
        progress="log",
    )

    info = pipeline.run(billing_raw_source())
    print(info)

    dest_counts = _count_postgres()
    print(f"Postgres {POSTGRES_URL.split('@')[-1]} schema `{POSTGRES_DATASET}` rows:", dest_counts)

    mismatches = {
        table: (source_counts[table], dest_counts.get(table, -1))
        for table in RAW_TABLES
        if dest_counts.get(table) != source_counts[table]
    }
    if mismatches:
        raise SystemExit(f"row-count mismatch MySQL vs Postgres: {mismatches}")

    print("dlt load verified: raw_customers, raw_subscriptions, raw_invoices")


if __name__ == "__main__":
    load_billing_to_postgres()
