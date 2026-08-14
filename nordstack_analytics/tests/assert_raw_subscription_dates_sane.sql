-- Catch planted end-before-start on RAW (warn: quarantine flags it).
{{ config(severity='warn', store_failures=true, tags=['raw']) }}

select
    subscription_id,
    start_date,
    end_date
from {{ source('billing_raw', 'raw_subscriptions') }}
where end_date is not null
  and end_date < start_date
