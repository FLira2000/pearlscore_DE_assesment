-- Business rule: subscription end_date must not precede start_date in clean staging.
-- Invalid ranges are excluded or flagged; this asserts no invalid ranges remain unflagged.
{{ config(tags=['staging']) }}

select
    subscription_id,
    start_date,
    end_date
from {{ ref('stg_subscriptions') }}
where end_date is not null
  and end_date < start_date
  and not is_invalid_date_range
