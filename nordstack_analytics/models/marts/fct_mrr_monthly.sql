{{
  config(
    alias='mrr_monthly',
    materialized='table',
    description='Monthly recurring revenue from paid invoices by month and plan (EUR).'
  )
}}

{#
  MRR proxy = sum of paid invoice amounts in EUR by invoice month and plan.
  FX: SEK converted in staging with var sek_to_eur_rate.
#}

select
    invoice_month,
    plan_name,
    count(*) as paid_invoice_count,
    sum(amount_eur) as mrr_eur
from {{ ref('stg_invoices') }}
where status = 'paid'
  and not coalesce(is_after_end_date, false)
group by 1, 2
