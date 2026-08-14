"""
NordStack end-to-end pipeline (CANDIDATE_BRIEF §0–§5).

Every 5 minutes:
  start_dag → load billing OLTP → dlt load analytics raw → dbt deps
  → test raw → build staging → build quarantine → build marts → end_dag
  → email on success; email on failure from any failed step (Mailpit SMTP).

`start_dag` / `end_dag` are EmptyOperators for UI skip / mark-success overrides.
Downstream tasks use NONE_FAILED so a skipped step does not block the rest.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

ROOT = os.environ.get("NORDSTACK_ROOT", "/opt/nordstack")
ALERT_EMAIL = os.environ.get("NORDSTACK_ALERT_EMAIL", "data-eng@nordstack.example")
PYTHON = os.environ.get("PYTHON_BIN", "python")
DBT_DIR = f"{ROOT}/nordstack_analytics"


def dbt_bash(command: str) -> str:
    return f"cd '{DBT_DIR}' && dbt {command} --profiles-dir ."


default_args = {
    "owner": "nordstack-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="nordstack_pipeline",
    description="Every 5 min: OLTP seed, dlt raw, dbt staging/quarantine/marts; email on success/fail.",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nordstack", "dlt", "dbt", "analytics"],
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")

    # NONE_FAILED: a manual skip upstream still lets this task run.
    continue_on_skip = {"trigger_rule": TriggerRule.NONE_FAILED}

    load_billing_oltp = BashOperator(
        task_id="load_billing_oltp",
        bash_command=(
            f"cd '{ROOT}' && "
            f"{PYTHON} scripts/wait_for_dbs.py mysql && "
            f"{PYTHON} scripts/seed_mysql.py"
        ),
        **continue_on_skip,
    )

    dlt_load_analytics_raw = BashOperator(
        task_id="dlt_load_analytics_raw",
        bash_command=(
            f"cd '{ROOT}' && "
            f"{PYTHON} scripts/wait_for_dbs.py postgres && "
            f"{PYTHON} pipelines/mysql_to_postgres.py"
        ),
        **continue_on_skip,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_bash("deps"),
        **continue_on_skip,
    )

    dbt_test_raw = BashOperator(
        task_id="dbt_test_raw",
        bash_command=dbt_bash("test --select source:billing_raw tag:raw"),
        **continue_on_skip,
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=dbt_bash("build --select tag:staging"),
        **continue_on_skip,
    )

    dbt_build_quarantine = BashOperator(
        task_id="dbt_build_quarantine",
        bash_command=dbt_bash("build --select tag:quarantine"),
        **continue_on_skip,
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=dbt_bash("build --select tag:marts"),
        **continue_on_skip,
    )

    end_dag = EmptyOperator(
        task_id="end_dag",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    email_success = EmailOperator(
        task_id="email_on_success",
        to=[ALERT_EMAIL],
        subject="[NordStack] pipeline succeeded",
        html_content=(
            "<p>DAG <b>{{ dag.dag_id }}</b> run <code>{{ run_id }}</code> succeeded.</p>"
            "<p>OLTP → dlt raw → dbt staging / quarantine / marts finished green.</p>"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    email_failure = EmailOperator(
        task_id="email_on_failure",
        to=[ALERT_EMAIL],
        subject="[NordStack] pipeline FAILED",
        html_content=(
            "<p>DAG <b>{{ dag.dag_id }}</b> run <code>{{ run_id }}</code> failed.</p>"
            "<p>Inspect the failed task logs in the Airflow UI.</p>"
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    (
        start_dag
        >> load_billing_oltp
        >> dlt_load_analytics_raw
        >> dbt_deps
        >> dbt_test_raw
        # Quarantine refs staging (e.g. qrt_invoices → stg_subscriptions); do not run in parallel.
        >> dbt_build_staging
        >> dbt_build_quarantine
        >> dbt_build_marts
        >> end_dag
        >> email_success
    )
    # ONE_FAILED must see the failed task as a direct upstream (not only via skip).
    [
        load_billing_oltp,
        dlt_load_analytics_raw,
        dbt_deps,
        dbt_test_raw,
        dbt_build_staging,
        dbt_build_quarantine,
        dbt_build_marts,
    ] >> email_failure
