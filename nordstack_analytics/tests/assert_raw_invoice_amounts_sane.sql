-- Catch planted negatives / null paid amounts on RAW (warn: staging quarantines them).
{{ config(severity='warn', store_failures=true, tags=['raw']) }}

select
    invoice_id,
    amount,
    status
from {{ source('billing_raw', 'raw_invoices') }}
where amount is null
   or amount <= 0
