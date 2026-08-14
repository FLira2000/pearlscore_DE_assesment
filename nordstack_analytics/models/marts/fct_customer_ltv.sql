{{
  config(
    alias='customer_ltv',
    materialized='table',
    description='Customer lifetime value to date from paid invoices, with plan mix and subscription status.'
  )
}}

with paid as (
    select
        s.customer_id,
        i.amount_eur,
        i.plan_name
    from {{ ref('stg_invoices') }} i
    inner join {{ ref('stg_subscriptions') }} s
        on i.subscription_id = s.subscription_id
    where i.status = 'paid'
      and not coalesce(i.is_after_end_date, false)
),

revenue as (
    select
        customer_id,
        sum(amount_eur) as ltv_eur,
        count(*) as paid_invoice_count
    from paid
    group by 1
),

plan_mix as (
    select
        customer_id,
        string_agg(distinct plan_name, ', ' order by plan_name) as plan_mix
    from paid
    group by 1
),

current_status as (
    select
        customer_id,
        string_agg(distinct status, ', ' order by status) as subscription_statuses,
        bool_or(status = 'active') as has_active_subscription
    from {{ ref('stg_subscriptions') }}
    group by 1
)

select
    c.customer_id,
    c.customer_name,
    c.country,
    coalesce(r.ltv_eur, 0) as ltv_eur,
    coalesce(r.paid_invoice_count, 0) as paid_invoice_count,
    pm.plan_mix,
    cs.subscription_statuses,
    coalesce(cs.has_active_subscription, false) as has_active_subscription
from {{ ref('stg_customers') }} c
left join revenue r on c.customer_id = r.customer_id
left join plan_mix pm on c.customer_id = pm.customer_id
left join current_status cs on c.customer_id = cs.customer_id
