{{ config(alias='subscriptions') }}

with source as (
    select * from {{ source('billing_raw', 'raw_subscriptions') }}
),

ranked as (
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
)

select
    s.subscription_id::varchar as subscription_id,
    s.customer_id::varchar as customer_id,
    s.plan_name::varchar as plan_name,
    s.monthly_price::numeric(12, 2) as monthly_price,
    s.start_date::date as start_date,
    s.end_date::date as end_date,
    lower(trim(s.status))::varchar as status,
    case
        when s._row_num > 1 then 'duplicate_subscription_id'
        when c.customer_id is null then 'orphan_customer_id'
        when s.monthly_price::numeric <= 0 then 'non_positive_monthly_price'
        when s.end_date is not null and s.end_date::date < s.start_date::date then 'end_before_start'
        else 'other'
    end as quarantine_reason
from ranked s
left join customers c on s.customer_id = c.customer_id
where s._row_num > 1
   or c.customer_id is null
   or s.monthly_price::numeric <= 0
   or (s.end_date is not null and s.end_date::date < s.start_date::date)
