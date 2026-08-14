#!/usr/bin/env bash
# One-shot bootstrap: Docker DBs → MySQL seed → dlt sync → dbt build
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${ROOT}/.venv/bin/python"
DBT="${ROOT}/.venv/bin/dbt"

if [[ ! -x "$PYTHON" ]]; then
  echo "missing $PYTHON — create the venv first (see README): python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi
if [[ ! -x "$DBT" ]]; then
  echo "missing $DBT — install host deps: .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi
export SOURCES__SQL_DATABASE__CREDENTIALS="${SOURCES__SQL_DATABASE__CREDENTIALS:-mysql+pymysql://billing_user:billing_password@127.0.0.1:3306/billing}"
export DESTINATION__POSTGRES__CREDENTIALS="${DESTINATION__POSTGRES__CREDENTIALS:-postgresql://dbt_user:dbt_password@127.0.0.1:5432/analytics}"
export MYSQL_URL="${MYSQL_URL:-$SOURCES__SQL_DATABASE__CREDENTIALS}"

cp .dlt/secrets.toml.example .dlt/secrets.toml
echo "refreshed .dlt/secrets.toml from example"

echo "==> starting postgres + mysql"
docker-compose up -d

echo "==> waiting for databases"
for i in {1..60}; do
  if docker-compose exec -T postgres pg_isready -U dbt_user -d analytics >/dev/null 2>&1 \
     && docker-compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -proot_password --silent >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [[ $i -eq 60 ]]; then
    echo "databases did not become healthy in time" >&2
    exit 1
  fi
done

echo "==> seeding MySQL from seed_data/"
"$PYTHON" scripts/seed_mysql.py

echo "==> dlt: MySQL billing → Postgres raw"
"$PYTHON" pipelines/mysql_to_postgres.py

echo "==> dbt deps + build"
cd nordstack_analytics
"$DBT" deps --profiles-dir .
"$DBT" build --profiles-dir .

echo "bootstrap complete."
