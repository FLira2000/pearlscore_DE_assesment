-- Business rule: paid invoices in staging must never have non-positive EUR amounts.
{{ config(tags=['staging']) }}

select
    invoice_id,
    amount_eur,
    status
from {{ ref('stg_invoices') }}
where status = 'paid'
  and amount_eur <= 0
