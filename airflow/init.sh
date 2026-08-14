#!/usr/bin/env bash
# Create Airflow metadata DB (same Postgres instance, database `airflow`)
# then migrate and ensure the admin user exists.
set -euo pipefail

python - <<'PY'
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="analytics",
    user="dbt_user",
    password="dbt_password",
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if cur.fetchone() is None:
    cur.execute("CREATE DATABASE airflow")
    print("created database airflow")
else:
    print("database airflow already exists")
cur.close()
conn.close()
PY

airflow db migrate

airflow users create \
  --username admin \
  --password admin \
  --firstname Nord \
  --lastname Stack \
  --role Admin \
  --email admin@nordstack.local \
  || true

echo "airflow init complete"
