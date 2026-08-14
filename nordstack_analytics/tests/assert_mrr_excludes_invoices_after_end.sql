-- Paid invoices billed after the subscription ended must be flagged
-- so MRR / LTV can exclude them.
{{ config(tags=['staging']) }}

select
    i.invoice_id,
    i.invoice_date,
    s.end_date
from {{ ref('stg_invoices') }} i
inner join {{ ref('stg_subscriptions') }} s
    on i.subscription_id = s.subscription_id
where i.status = 'paid'
  and s.end_date is not null
  and i.invoice_date > s.end_date
  and not coalesce(i.is_after_end_date, false)
