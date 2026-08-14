-- Billing rule: an active subscription must not have an end_date.
{{ config(tags=['staging']) }}

select
    subscription_id,
    status,
    end_date
from {{ ref('stg_subscriptions') }}
where status = 'active'
  and end_date is not null
