{{
  config(
    alias='churn_monthly',
    materialized='table',
    description='Subscriptions cancelled per month and list-price MRR lost (EUR assumed).'
  )
}}

select
    date_trunc('month', end_date)::date as churn_month,
    count(*) as cancelled_subscriptions,
    sum(monthly_price) as mrr_lost_eur
from {{ ref('stg_subscriptions') }}
where status = 'cancelled'
  and end_date is not null
  and not is_invalid_date_range
  and monthly_price > 0
group by 1
