{{ config(alias='customers') }}

{# Rows dropped or flagged from customers: duplicate extras + identity issues. #}

with source as (
    select * from {{ source('billing_raw', 'raw_customers') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by created_at, email
        ) as _row_num
    from source
)

select
    customer_id::varchar as customer_id,
    customer_name::varchar as customer_name,
    email::varchar as email,
    country::varchar as country,
    created_at::date as created_at,
    case
        when _row_num > 1 then 'duplicate_customer_id'
        when email is null or email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then 'invalid_email'
        when country is null or nullif(trim(country), '') is null then 'missing_country'
        else 'other'
    end as quarantine_reason
from ranked
where _row_num > 1
   or email is null
   or email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
   or country is null
   or nullif(trim(country), '') is null
