# Approach & decision log

How this NordStack take-home was built: train of thought, what we chose and why, where an AI assistant was used, and where human review pushed back and changed the design.

This is not a second README. Setup and DQ tables live in [`README.md`](README.md); planted-issue inventory lives in [`DATA_EXPLORATION.md`](DATA_EXPLORATION.md). This file is the *reasoning* trail.

---

## 1. Starting point

The brief asks for a trustworthy analytics layer on dirty billing CSVs, with optional dlt (MySQL → Postgres), dbt (staging + marts + tests), docs, and an Airflow DAG every 5 minutes with email on success/fail.

Working assumption from the start:

- Treat the CSVs as hostile. Profile first; model second.
- Prefer a path a reviewer can reproduce in one or two commands.
- Optional §0 (dlt) was worth doing: it makes Postgres seeding “free” and shows a real EL shape, not only dbt-on-CSV.

Exploration (`DATA_EXPLORATION.md`) locked the planted issues before any mart was written: duplicates, orphans, bad email/country, status whitespace, negatives/nulls, SEK, inverted dates, invoices after end. That list drove quarantine reasons and which tests warn on raw vs error on staging/marts.

---

## 2. Architecture we landed on

```
seed CSVs → MySQL `billing` (OLTP source)
         → dlt → Postgres `analytics.raw`
         → dbt staging / quarantine / marts
         → Airflow orchestrates the whole chain every 5 min + email
```

| Piece | Choice | Why |
|---|---|---|
| Source OLTP | MySQL 8 in Compose | Brief optional §0; keeps “billing system” separate from the warehouse |
| Warehouse | Postgres 16 as given | Match the brief credentials/ports; don’t invent a second store |
| Extract/load | dlt `replace` into schema `raw` | Full refresh is honest for ~3k invoice rows; no fake incremental |
| Transform | dbt project `nordstack_analytics` | Staging views + quarantine views + mart tables |
| Orchestration | Airflow 2.10 in Compose + Mailpit | Meets §5; local SMTP without real credentials |
| Bootstrap | `scripts/bootstrap.sh` | Clone → DBs → seed → dlt → `dbt build` for reviewers who skip the UI |

**Schemas:** custom `generate_schema_name` so layers are literally `raw` / `staging` / `quarantine` / `marts`, not `analytics_staging`. Reviewers should see clean names in Postgres.

**Mart definitions (kept deliberately simple):**

- **MRR** = sum of *paid* invoice amounts in EUR by month × plan (cash proxy, not classic book MRR). SEK → EUR at hardcoded `0.087`. Drop invoices after subscription `end_date`.
- **LTV** = paid EUR to date per customer + country + plan mix + active flag. Bad email/null country stay in grain and get flagged/quarantined.
- **Churn** = cancelled subs by `end_date` month + list-price MRR lost; exclude invalid date ranges and non-positive prices.

dlt does **not** clean. Cleaning ownership sits in dbt so planted defects remain visible in raw/quarantine.

---

## 3. Data quality strategy

Brief requirement: raw/staging tests should *catch* planted issues; marts tests should *pass*; `dbt build` green without deleting tests.

| Layer | Severity | Intent |
|---|---|---|
| Raw sources + singular raw tests | `warn` | Prove we see dupes, orphans, bad amounts/dates |
| Staging / quarantine / marts | `error` | Enforce clean contracts for BI |
| `assert_planted_issues_quarantined` | `error` | Fail if quarantine stops catching known defect classes |

Generic tests (unique, not_null, accepted_values, relationships) plus several singular SQL tests (>2 required). Handling per issue is tabulated in the README — fix (normalize status), quarantine (orphans, negatives), flag-and-keep (invalid email for LTV grain), convert (SEK).

That split is intentional: warn-on-raw is how you get a green build *and* evidence of dirt.

---

## 4. Airflow: train of thought and course corrections

§5 is short: run dbt every 5 minutes; email on success/fail. We built more than the minimum (full ELT in the DAG) because the optional dlt path only makes sense if orchestration owns seed → dlt → dbt, not just `dbt build` in isolation.

### What the AI first proposed (and what changed)

Work was done with an AI coding assistant (Cursor). It moved fast on scaffolding (Compose, dlt, dbt layers, DAG). Several early defaults were wrong for how *I* wanted the graph to read or operate. Below: assistant suggestion → disagreement → final.

| Topic | AI / first pass | Disagreement | After |
|---|---|---|---|
| Schedule | `*/5 * * * *` to match the brief | I asked for **one-shot / manual only** while iterating — constant 5‑min runs were noisy and expensive locally | Temporarily `schedule=None`; later **restored every 5 min** when we deliberately closed the brief gap for §5 |
| Email tasks | `EmailOperator` success + failure (+ Mailpit) | I asked to **remove** email steps while redesigning the graph | Removed; then **brought back** with Mailpit when implementing §5 “as asked”, without dropping the blank bookends |
| Wait tasks | Explicit `wait_postgres` / `wait_mysql` | Names felt like infra noise; I wanted steps that sound like *work* (load / dlt), not healthchecks | Folded readiness into `load_billing_oltp` and `dlt_load_analytics_raw` |
| Naming | Engine-centric (`seed_mysql`, `dlt_mysql_to_postgres`) | Prefer **role** over engine in the DAG UI | `load_billing_oltp`, `dlt_load_analytics_raw`, etc. Script filenames stayed MySQL/Postgres-oriented (I clarified renaming was for **Airflow steps**, not necessarily the Python modules) |
| dbt in one task | Single `dbt build` | I asked to **separate model types** in Airflow | `dbt_test_raw` → `dbt_build_staging` → `dbt_build_quarantine` → `dbt_build_marts` with tags |
| Parallel staging ∥ quarantine | Fan-out after raw tests | Looked nice; **raced** because quarantine refs staging (`staging.subscriptions does not exist`) — easy to misread as a “relationship” test failure | **Serialized**: staging then quarantine then marts |
| Control points | Not present | I wanted blank **start/end** for UI skip / mark-success overrides | `start_dag` / `end_dag` `EmptyOperator`s; work tasks use `NONE_FAILED` so a skip doesn’t kill the run |
| Email after end | Email only after full success path | Keep bookends; still satisfy brief | `… → end_dag → email_on_success`; `email_on_failure` still wired from each work task with `ONE_FAILED` |

### Final DAG shape

```
start_dag
  → load_billing_oltp
  → dlt_load_analytics_raw
  → dbt_deps
  → dbt_test_raw
  → dbt_build_staging
  → dbt_build_quarantine
  → dbt_build_marts
  → end_dag
  → email_on_success
(+ email_on_failure from any failed work task)
```

**Tradeoff I accept:** a 5‑minute schedule with full seed+dlt+dbt is heavy for a take-home demo. `max_active_runs=1` and `catchup=False` limit pile-up. For production I’d schedule extract less often than marts, and make seed a manual/backfill-only step.

---

## 5. Engineering friction we actually hit

These shaped the repo as much as the brief:

1. **Airflow image vs SQLAlchemy** — Airflow 2.10 needs SQLAlchemy 1.4; pinning SA 2.x in the image broke the webserver. Extra packages go in `airflow/requirements.txt` without upgrading SQLAlchemy; host `requirements.txt` can differ.
2. **pandas `to_sql` under Airflow’s SA 1.4** — seed via pandas failed (`no attribute cursor`). Seeder rewritten to **PyMySQL + CSV** so seed doesn’t depend on that stack fight.
3. **CRLF on `bootstrap.sh`** — `env: bash\r` on macOS. Normalized to LF; `.gitattributes` keeps shell/Python as LF.
4. **dbt profiles from the venv** — running `dbt` from the repo root without `--profiles-dir .` looks for missing `~/.dbt`. Profiles themselves (`profile: nordstack` → local Postgres) were fine; cwd + profiles-dir were the footgun. Documented; docs serve on **8081** so it doesn’t steal Airflow’s **8080**.

None of these are “modeling” wins, but they are what makes `./scripts/bootstrap.sh` and the DAG reviewable.

---

## 6. How AI was used (candid)

**Used for:** Compose/Airflow wiring, dlt pipeline skeleton, dbt model/test boilerplate, quarantine patterns, DAG edits, grepping logs, README scaffolding, fixing environment breakages.

**Not a substitute for:** deciding MRR vs cash semantics, what to quarantine vs flag-and-keep, whether §5 should temporarily be manual, or whether parallel layer builds were worth the race. Those calls were reviewed and, in several cases, reversed (table in §4).

**Bias to watch:** the assistant optimized for “complete the brief checklist” (5‑min schedule, email, parallel DAG beauty). I pulled toward operable local UX (manual runs, fewer wait nodes, clearer task names, blank start/end), then **re-aligned to the brief** for schedule + email once the graph felt right. That push–pull is recorded in this file (§4), not in a long public commit history.

---

## 7. What I’d still change with more time

Same spirit as README “next steps,” sharpened:

- Stop reseeding MySQL every 5 minutes; seed once (or on demand), dlt incremental merge on `invoice_id`.
- True subscription SCD-2 if finance wants classic MRR, not paid-invoice cash.
- Real SMTP / Slack instead of Mailpit; alert on quarantine row-count spikes, not only DAG fail.
- Rate table for FX instead of a hardcoded SEK factor.
- CI job: `bootstrap` or `dbt build` on PR so green isn’t only a laptop claim.

### Airflow operators: leave Bash behind

Today every work step is a `BashOperator` wrapping `python` / `dbt`. That was fine for a 3–4h take-home (one file, easy to follow), but it is weak for code review and brittle at the edges: opaque exit codes, no typed XCom, hard to unit-test, and connection secrets live in env strings instead of Airflow Connections.

With more time I would reshape the DAG around **purpose-built operators** (or thin `PythonOperator` callables that call the same libraries), so the graph reads as contracts instead of shell:

| Concern | Prefer over `BashOperator` | Why it is more stable / reviewable |
|---|---|---|
| **SQL / DB readiness & seed** | `PostgresOperator` / `MySqlOperator` (or `@task` + SQLAlchemy with Airflow Connections `postgres_analytics` / `mysql_billing`) | DDL/DML and `pg_isready`-style checks become visible SQL in the task; credentials sit in Connections/Secrets Backend; reviewers see statements, not `bash -c` |
| **dlt extract/load** | Custom operator or `@task` that imports `pipelines.mysql_to_postgres.load_billing_to_postgres` (or [dlt’s Airflow helpers](https://dlthub.com/) if pinned) | Pipeline code is a normal Python function under test; Airflow only schedules/retires; XCom can carry row counts / load package id for downstream gates |
| **dbt** | `DbtRunOperator` / `DbtTestOperator` / `DbtBuildOperator` from `astronomer-cosmos` or `airflow-dbt-python` (one task per select: raw tests, staging, quarantine, marts) | Manifest-aware selection, clearer logs, reusable profiles connection, and DAG structure that mirrors dbt layers without embedding CLI strings |

Concrete follow-ons once those operators exist:

- **Connection objects** for MySQL, Postgres, and SMTP instead of Compose env URLs alone — same DAG runs in another env by swapping Connection hosts.
- **Sensor or SQL check tasks** (e.g. `SQLExecuteQueryOperator` asserting `raw` row counts match source) between dlt and dbt, so a silent empty load fails before marts.
- **TaskFlow + typed returns** (load metadata → dbt vars) so reviewers can trace data contracts in Python, not parse bash logs.
- Keep `start_dag` / `end_dag` EmptyOperators; swap only the middle Bash nodes so manual skip/override behavior stays.

That shift is the main architectural upgrade I’d make next: Bash was a deliberate shortcut; dedicated SQL / dlt / dbt operators are what I’d want in a PR review for anything past the assessment.
---

## 8. Mapping back to the brief

| Section | Status |
|---|---|
| §0 dlt MySQL → Postgres | Done |
| §1 seed Postgres | Skipped via §0 (dlt loads `raw`) |
| §2 staging + MRR / LTV / churn | Done |
| §3 tests + DQ in README | Done (`dbt build` green with raw warns) |
| §4 README + mart descriptions | Done |
| §5 Airflow every 5 min + email | Done (plus start/end blanks and layered dbt steps) |

Submit artifact set: dbt project, seed/dlt scripts, Compose (Postgres + MySQL + Airflow + Mailpit), DAG, README, `packages.yml`, plus this decision log and the exploration notes.
