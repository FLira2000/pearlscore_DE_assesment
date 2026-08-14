{{ config(alias='customers') }}

{#
  Staging customers:
  - Deduplicate customer_id (keep first by created_at, email).
  - Normalize whitespace / country case.
  - Invalid email and missing country: keep for LTV, flag + quarantine audit.
  - Future created_at (C0041): keep, flag as timeline issue.
#}

with source as (
    select * from {{ source('billing_raw', 'raw_customers') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by created_at, email
        ) as _row_num
    from source
),

typed as (
    select
        customer_id::varchar as customer_id,
        nullif(trim(customer_name), '')::varchar as customer_name,
        nullif(trim(email), '')::varchar as email,
        nullif(upper(trim(country)), '')::varchar as country,
        created_at::date as created_at,
        (
            email is null
            or email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
        ) as is_invalid_email,
        (country is null or nullif(trim(country), '') is null) as is_missing_country,
        (created_at::date > current_date) as is_future_created_at
    from deduped
    where _row_num = 1
)

select
    customer_id,
    customer_name,
    email,
    coalesce(country, 'UNKNOWN') as country,
    created_at,
    is_invalid_email,
    is_missing_country,
    is_future_created_at,
    (is_invalid_email or is_missing_country or is_future_created_at) as has_quality_issue
from typed
