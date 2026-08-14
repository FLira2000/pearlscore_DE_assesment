# NordStack Billing Data — Exploration Insights

Exploratory profiling of the three raw CSV exports in `seed_data/`, run against the local files before any cleaning or dbt modeling. Counts below are **raw** unless noted.

**Snapshot:** 121 customer rows · 175 subscription rows · 2,855 invoice rows · invoice window `2024-01` → `2026-07` (31 months).

---

## 1. Grain & coverage

| Source | Expected grain | Raw rows | Distinct key | Notes |
|---|---|---|---|---|
| `raw_customers.csv` | 1 row / customer | 121 | 120 `customer_id` | Exact duplicate of `C0023` |
| `raw_subscriptions.csv` | 1 row / subscription | 175 | 174 `subscription_id` | Exact duplicate of `S00006` |
| `raw_invoices.csv` | 1 row / invoice | 2,855 | 2,855 `invoice_id` | No duplicate invoice IDs |

**Referential coverage**

- Every real customer has ≥1 subscription (after ignoring planted orphan `C9999`).
- 42 customers have multiple subscriptions (max 3); **12** have more than one **active** subscription at once (plan stacking / upgrades possible).
- 1 subscription has **no invoices**: `S00149` (`scale`, starts `2026-10-05` — future vs. exploration date).
- Planted orphans: subscription `S00011` → `C9999`; invoice `I000601` → `S99999`.

---

## 2. Planted / clear data-quality issues

These are the deliberate defects the assessment expects staging tests to catch. Recommended handling is a first pass for the dbt layer (quarantine vs. exclude vs. fix).

| # | Issue | Evidence | Suggested handling |
|---|---|---|---|
| 1 | Duplicate customer | `C0023` / Sofia Santos duplicated (rows 23 & 121) | Deduplicate on `customer_id` (keep one) |
| 2 | Duplicate subscription | `S00006` duplicated exactly | Deduplicate on `subscription_id` |
| 3 | Orphan subscription FK | `S00011` → `C9999` (not in customers) | Quarantine / exclude from marts |
| 4 | Orphan invoice FK | `I000601` → `S99999` | Quarantine / exclude from marts |
| 5 | Invalid email | `C0016` email = `not-an-email` | Flag / quarantine; keep id if revenue exists |
| 6 | Null country | `C0008` (`Mateo Meyer`) | Default `unknown` or quarantine for geo marts |
| 7 | Case / whitespace on status | Sub `S00026` status `ACTIVE`; invoice `I000451` status `'PAID '` | `lower(trim())` in staging |
| 8 | Negative list price | `S00048` starter `@ -99.0` (cancelled) | Exclude from MRR; quarantine |
| 9 | Negative invoice amounts | 7 invoices on `S00048`, all `-99.0` | Exclude from paid revenue / MRR |
| 10 | Null invoice amount | `I000322` (`paid`, EUR, amount null) | Quarantine; never count as revenue |
| 11 | Non-EUR currency | 2 invoices in `SEK` (`I000101` paid, `I000201` open) | Convert with documented FX or quarantine |
| 12 | End date before start | `S00034`: start `2025-03-29`, end `2025-03-19` | Quarantine dates; drives invoice-after-end noise |
| 13 | Invoices after end date | 17 invoices on `S00034` after end | Exclude from post-churn MRR or fix end date upstream |
| 14 | Future customer create date | `C0041` `created_at = 2027-03-15` while sub `S00054` starts `2024-06-24` | Flag timeline violation |
| 15 | Future churn dates | Cancelled ends in `2027-01` / `2027-03` | Keep but document as future-dated events |

**Not issues (valid domain values)**

- Plans are only `starter` / `growth` / `scale` with list prices `29` / `99` / `299` (except the planted `-99`).
- Countries are ISO-like EU codes: `DE, NL, FR, ES, SE, IT, NO, PT, FI, PL` (+ 1 null).
- Invoice statuses (after normalize): `paid`, `failed`, `open`.
- Subscription statuses (after normalize): `active`, `cancelled`, `paused`.
- Active subscriptions correctly have empty `end_date`; cancelled always have one (except quality cases above).

---

## 3. Business snapshot (exploratory, not final marts)

Figures below apply light cleaning for readability only: strip/lower status, drop exact duplicate rows, drop orphan `S99999` invoice, keep EUR, require `amount > 0` and `status = paid`. **Not** a substitute for dbt models.

### Revenue

- **Paid EUR cash collected (proxy):** ~€325,256 across 2,444 invoices.
- **By plan (paid EUR):** scale €211k (65%) · growth €90k (28%) · starter €24k (8%).
- Scale has fewer paid invoices than starter/growth but dominates revenue because of unit price.
- Monthly paid EUR rises from near-zero in early 2024 to a plateau around **€14–15k / month** through mid-2026.

### Active book (list-price MRR proxy)

- **102 active** subscriptions after status normalize (`ACTIVE` → active).
- Active list-price MRR ≈ **€13,638** (excluding the negative-price sub, which is cancelled).
- Mix: starter 38 · growth 33 · scale 31 — scale still ~68% of active MRR.
- **21 paused** (not cancelled, not contributing if MRR = paid only).

### Churn

- **52 cancelled** subscriptions; list-price MRR associated ≈ €7,640 (includes the `-99` starter, which distorts one month).
- Cancellations span `2024-06` → `2027-03`; denser from late 2025 onward.
- For mart design: churn month = `date_trunc('month', end_date)`; MRR lost = subscription `monthly_price` in reporting currency, only for valid positive prices.

### Customer value

- All 120 distinct customers have some paid EUR after filters (min €29, median ~€1,980, max ~€11,362 on `C0038`).
- Top revenue countries by paid EUR: **DE → NL → PL → PT → SE**. Poland punches above customer count (7 customers, ~€35k).

### Invoice health

- ~7.1% `failed`, ~7.0% `open`, remainder `paid` (after normalize).
- No duplicate `(subscription_id, invoice_date)` pairs — monthly billing looks consistent at that grain.
- Invoice amounts almost always match subscription `monthly_price` when the FK resolves; mismatches are the null amount and the orphan invoice.

---

## 4. Modeling implications for dbt

1. **Staging must normalize enums** (`trim` + `lower`) before accepted-value tests, or tests will fail on `ACTIVE` / `PAID `.
2. **Deduplicate** customers and subscriptions before uniqueness tests on marts.
3. **Quarantine models** (or `where` filters + audit tables) for: orphan FKs, negative amounts/prices, null amount, invalid email, broken date ranges.
4. **MRR definition:** sum of **paid** invoices in reporting currency by `invoice_month` × `plan_name`. State FX explicitly for SEK (e.g. hardcoded rate with comment) or exclude SEK until rates exist.
5. **LTV:** sum of paid revenue per `customer_id`, join country + current subscription status / plan mix from cleaned subscriptions.
6. **Churn view:** count cancelled subs and sum positive `monthly_price` by end-month; exclude orphan `C9999` / invalid prices.
7. **Singular tests worth writing:** no paid invoice with `amount <= 0`; no invoice without a known subscription; no `end_date < start_date`; no active sub with `end_date` set.

---

## 5. Open questions for product / finance

- Are multiple concurrent active subscriptions per customer intentional (upsell) or a billing bug?
- Should `paused` reduce MRR to zero immediately, or only when invoices stop?
- Is SEK a real billing currency for NordStack, or seed noise?
- How should credits/refunds appear? Today negatives look like bad seed data, not documented credits.
- Customer `created_at` in 2027 — clock skew in the export, or intentional future seed?

---

## 6. Reproduction

```bash
# from repo root (optional local venv)
python3 -m venv .venv
.venv/bin/pip install pandas
.venv/bin/python -c "import pandas as pd; \
  print({p: len(pd.read_csv(f'seed_data/{p}')) for p in \
  ['raw_customers.csv','raw_subscriptions.csv','raw_invoices.csv']})"
```

Full profiling used pandas on the three CSVs; no database load was required for this document.
