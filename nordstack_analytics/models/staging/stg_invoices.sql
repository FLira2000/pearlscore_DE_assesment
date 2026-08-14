{{ config(alias='invoices') }}

{#
  Staging invoices:
  - Normalize status (trim + lower): 'PAID ' → paid.
  - Convert SEK → EUR using var sek_to_eur_rate (hardcoded FX).
  - Exclude orphan subscription_id and null/non-positive amounts → quarantine.
  - Flag invoices after subscription end_date (S00034); marts exclude them from MRR/LTV.
  - Keep failed/open rows; marts filter to paid for MRR.
#}

with source as (
    select * from {{ source('billing_raw', 'raw_invoices') }}
),

subscriptions as (
    select subscription_id, plan_name, start_date, end_date
    from {{ ref('stg_subscriptions') }}
),

typed as (
    select
        i.invoice_id::varchar as invoice_id,
        i.subscription_id::varchar as subscription_id,
        i.invoice_date::date as invoice_date,
        i.amount::numeric(12, 2) as amount_original,
        upper(trim(i.currency))::varchar as currency_original,
        lower(trim(i.status))::varchar as status,
        (s.subscription_id is null) as is_orphan_subscription,
        (i.amount is null or i.amount::numeric <= 0) as is_invalid_amount,
        (
            s.end_date is not null
            and i.invoice_date::date > s.end_date
        ) as is_after_end_date,
        case
            when upper(trim(i.currency)) = 'EUR' then i.amount::numeric(12, 2)
            when upper(trim(i.currency)) = 'SEK'
                then round(i.amount::numeric * {{ var('sek_to_eur_rate') }}, 2)
            else null
        end as amount_eur,
        s.plan_name
    from source i
    left join subscriptions s on i.subscription_id = s.subscription_id
)

select
    invoice_id,
    subscription_id,
    invoice_date,
    date_trunc('month', invoice_date)::date as invoice_month,
    amount_original,
    currency_original,
    amount_eur,
    '{{ var("reporting_currency") }}'::varchar as reporting_currency,
    status,
    plan_name,
    is_orphan_subscription,
    is_invalid_amount,
    is_after_end_date,
    (
        is_orphan_subscription
        or is_invalid_amount
        or amount_eur is null
        or is_after_end_date
    ) as has_quality_issue
from typed
where not is_orphan_subscription
  and not is_invalid_amount
  and amount_eur is not null
