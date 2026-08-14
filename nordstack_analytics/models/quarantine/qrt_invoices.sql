{{ config(alias='invoices') }}

{#
  Quarantine invoices. Reason priority matters: amount/currency before
  "missing from staging subscriptions", otherwise invoices on excluded
  subs (e.g. S00048 negative price) are mislabeled as orphan_subscription_id.
#}

with source as (
    select * from {{ source('billing_raw', 'raw_invoices') }}
),

subscriptions as (
    select subscription_id, end_date from {{ ref('stg_subscriptions') }}
)

select
    i.invoice_id::varchar as invoice_id,
    i.subscription_id::varchar as subscription_id,
    i.invoice_date::date as invoice_date,
    i.amount::numeric(12, 2) as amount,
    upper(trim(i.currency))::varchar as currency,
    lower(trim(i.status))::varchar as status,
    case
        when i.amount is null then 'null_amount'
        when i.amount::numeric <= 0 then 'non_positive_amount'
        when upper(trim(i.currency)) not in ('EUR', 'SEK') then 'unsupported_currency'
        when s.subscription_id is null then 'orphan_subscription_id'
        when s.end_date is not null and i.invoice_date::date > s.end_date then 'invoice_after_end_date'
        else 'other'
    end as quarantine_reason
from source i
left join subscriptions s on i.subscription_id = s.subscription_id
where i.amount is null
   or i.amount::numeric <= 0
   or upper(trim(i.currency)) not in ('EUR', 'SEK')
   or s.subscription_id is null
   or (s.end_date is not null and i.invoice_date::date > s.end_date)
