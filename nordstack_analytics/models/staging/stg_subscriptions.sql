{{ config(alias='subscriptions') }}

{#
  Staging subscriptions:
  - Deduplicate subscription_id.
  - Normalize status (trim + lower): ACTIVE → active.
  - Exclude orphan customer_id (not in cleaned customers) → quarantine.
  - Exclude negative / non-positive monthly_price → quarantine.
  - Flag end_date < start_date (keep row but mark; marts exclude from churn MRR).
#}

with source as (
    select * from {{ source('billing_raw', 'raw_subscriptions') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by subscription_id
            order by start_date, status
        ) as _row_num
    from source
),

customers as (
    select customer_id from {{ ref('stg_customers') }}
),

typed as (
    select
        s.subscription_id::varchar as subscription_id,
        s.customer_id::varchar as customer_id,
        lower(trim(s.plan_name))::varchar as plan_name,
        s.monthly_price::numeric(12, 2) as monthly_price,
        s.start_date::date as start_date,
        s.end_date::date as end_date,
        lower(trim(s.status))::varchar as status,
        (c.customer_id is null) as is_orphan_customer,
        (s.monthly_price::numeric <= 0) as is_invalid_price,
        (
            s.end_date is not null
            and s.start_date is not null
            and s.end_date::date < s.start_date::date
        ) as is_invalid_date_range
    from deduped s
    left join customers c on s.customer_id = c.customer_id
    where s._row_num = 1
)

select
    subscription_id,
    customer_id,
    plan_name,
    monthly_price,
    start_date,
    end_date,
    status,
    is_orphan_customer,
    is_invalid_price,
    is_invalid_date_range,
    (is_orphan_customer or is_invalid_price or is_invalid_date_range) as has_quality_issue
from typed
where not is_orphan_customer
  and not is_invalid_price
