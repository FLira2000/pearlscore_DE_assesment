# NordStack analytics layer

Take-home data platform for a fictional B2B SaaS (plans `starter` / `growth` / `scale`). Raw billing CSVs are loaded through **MySQL → dlt → Postgres**, then modeled in **dbt** with staging, quarantine, and marts. An **Airflow DAG** runs the pipeline every 5 minutes and emails on success or failure.

Warehouse (from `CANDIDATE_BRIEF.md`): **Postgres 16** at `localhost:5432`, database `analytics`, user `dbt_user`, password `dbt_password`.

Decision log / train of thought (including where AI drafts were pushed back): [`APPROACH.md`](APPROACH.md). Exploration notes: [`DATA_EXPLORATION.md`](DATA_EXPLORATION.md).

---

## Setup

Requires Docker, Python 3.10+ (3.12 used here), and a free `5432` / `3306` / `8080` / `8081` / `8025` on the host.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
cp .dlt/secrets.toml.example .dlt/secrets.toml
./scripts/bootstrap.sh
```

`bootstrap.sh` starts **Postgres, MySQL, Mailpit, and Airflow** (builds the Airflow image), seeds MySQL, runs dlt into Postgres `raw`, then `dbt build`.

- Airflow UI: http://localhost:8080 — user `admin` / password `admin`
- Mailpit (DAG emails): http://localhost:8025
- DAG `nordstack_pipeline` runs **every 5 minutes**: `start_dag` → `load_billing_oltp` → `dlt_load_analytics_raw` → `dbt_deps` → `dbt_test_raw` → `dbt_build_staging` → `dbt_build_quarantine` → `dbt_build_marts` → `end_dag` → email. Skip a task in the UI to override that step; `NONE_FAILED` lets the rest continue.

Useful follow-ups (always from `nordstack_analytics/`; profile name is `nordstack` in `profiles.yml`):

```bash
source .venv/bin/activate
cd nordstack_analytics
dbt debug --profiles-dir .          # should print: All checks passed!
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8081   # 8080 is Airflow
```

Open docs at http://localhost:8081. If you see `Path '.../.dbt' does not exist`, either pass `--profiles-dir .` as above, or `mkdir -p ~/.dbt` and symlink this project's `profiles.yml` there. Do not run dbt from the repo root — it needs `dbt_project.yml` in the cwd.

---

## Project structure

```
seed_data/                       # source CSVs
mysql/init/                      # MySQL billing DDL (dlt source)
scripts/seed_mysql.py            # CSV → MySQL
scripts/wait_for_dbs.py          # readiness checks (used by DAG / bootstrap)
scripts/bootstrap.sh             # one-command path from clone → green dbt
pipelines/mysql_to_postgres.py   # dlt: MySQL billing → Postgres raw
.dlt/secrets.toml.example        # copy to secrets.toml (gitignored)
requirements.txt                 # host venv (dlt, dbt, pandas, …)
nordstack_analytics/             # dbt project
  profiles.yml                   # profile `nordstack` → Postgres analytics
  packages.yml                   # dbt_utils
  models/staging/                # views: type, clean, flag
  models/quarantine/             # views: rejected / audit rows
  models/marts/                  # tables: MRR, LTV, churn
  tests/                         # singular business-rule tests
dags/nordstack_pipeline.py       # Airflow: start → OLTP → dlt → dbt layers → end → email
airflow/Dockerfile               # Airflow image with dlt + dbt (see airflow/requirements.txt)
airflow/init.sh                  # metadata DB + admin user
airflow/requirements.txt         # packages baked into the Airflow image (not host pip)
```

Host Python uses `requirements.txt`. Airflow runs only via Compose and installs `airflow/requirements.txt` into the image — do not `pip install -r requirements-airflow.txt` expecting a local Airflow.
---

## Modeling decisions

| Layer | Materialization | Why |
|---|---|---|
| Staging | view | Cheap to recompute; always reflects latest `raw`. |
| Quarantine | view | Audit of planted issues without copying data. |
| Marts | table | Small aggregates; stable for BI / Airflow. |
| Schemas | `raw` / `staging` / `quarantine` / `marts` | Custom `generate_schema_name` so dbt does not prefix `analytics_`. |

**MRR** = sum of **paid** invoice amounts in **EUR**, by `invoice_month` × `plan_name`.  
**FX:** `SEK → EUR` at hardcoded `sek_to_eur_rate = 0.087` (`dbt_project.yml`). Only two SEK invoices exist; this is an illustrative rate, not a live feed. Invoices billed after `end_date` are excluded.

**LTV** = paid EUR to date per customer, plus country, distinct plan mix, and whether they currently have an active subscription. Invalid email / missing country stay in the grain (flagged).

**Churn** = cancelled subscriptions by `date_trunc('month', end_date)`, with list-price MRR lost. Invalid date ranges and non-positive prices are excluded.

dlt does **not** clean data. Staging / quarantine own quality so planted defects remain visible.

---

## Data-quality findings and handling

| Issue | Handling |
|---|---|
| Duplicate customer `C0023` | Dedupe in `stg_customers`; extra row → `qrt_customers` |
| Duplicate subscription `S00006` | Dedupe in `stg_subscriptions`; extra row → quarantine |
| Orphan sub `S00011` → `C9999` | Exclude from staging; quarantine `orphan_customer_id` |
| Orphan invoice `I000601` → `S99999` | Exclude from staging; quarantine `orphan_subscription_id`. Invoices whose subscription was itself excluded from staging (e.g. orphan sub `S00011`) also land here. |
| Invalid email `C0016` | Keep in staging (flag); quarantine `invalid_email` |
| Null country `C0008` | Coalesce to `UNKNOWN`; quarantine `missing_country` |
| Status `ACTIVE` / `'PAID '` | `lower(trim())` in staging so accepted-value tests pass |
| Negative price `S00048` (`-99`) | Exclude from staging; quarantine |
| Negative invoice amounts (7 rows) | Exclude from staging / MRR; quarantine `non_positive_amount` |
| Null paid amount `I000322` | Quarantine; never counted as revenue |
| SEK invoices (2) | Convert with `sek_to_eur_rate`; stay in staging |
| End before start `S00034` | Flag `is_invalid_date_range`; exclude from churn MRR; quarantine |
| Invoices after end (17 on `S00034`) | Flag `is_after_end_date`; drop from MRR/LTV; quarantine |
| Future `created_at` on `C0041` | Keep; flag `is_future_created_at` |
| Future churn dates (2027) | Keep in churn (valid cancelled rows) |

**How tests catch vs pass**

- **Raw (warn):** unique `customer_id` / `subscription_id`, FK relationships, singular tests for negative amounts and inverted dates. These *fail the assertion* on planted rows but use `severity: warn` so `dbt build` stays green after upstream handling.
- **Quarantine (error):** `assert_planted_issues_quarantined` requires each planted defect class to be present.
- **Staging / marts (error):** uniqueness, not-null, accepted values, relationships, `amount_eur > 0`, no active sub with `end_date`, paid invoices never ≤ 0.

`dbt build` must be green: failures are quarantined or normalized, not deleted by removing tests.

---

## What I'd do next

- Confirm with finance whether concurrent active subscriptions are upsells or bugs; if bugs, add a uniqueness test on active sub per customer.
- Replace the hardcoded SEK rate with a rates table (or ECB feed).
- Incremental dlt (`write_disposition=merge` on `invoice_id`) instead of full replace.
- Snapshot subscriptions for true SCD-2 MRR (this mart is invoice-cash, not classic beginning-of-month book).
- Wire a real SMTP server instead of Mailpit; add a Slack webhook alongside email.
- Replace DAG `BashOperator`s with purpose-built **SQL / dlt / dbt operators** (Connections + Cosmos or airflow-dbt-python + a thin dlt `@task`) for clearer reviews and a stabler architecture — detail in [`APPROACH.md`](APPROACH.md) §7.
- dbt exposures + a thin Looker/Metabase layer on `marts.*`.
