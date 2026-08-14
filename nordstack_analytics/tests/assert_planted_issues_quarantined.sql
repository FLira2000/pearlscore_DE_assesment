-- Planted quality issues must land in quarantine (caught upstream of marts).
{{ config(tags=['quarantine']) }}

select 'duplicate_customer_id' as missing_quarantine_reason
where not exists (
    select 1 from {{ ref('qrt_customers') }} where quarantine_reason = 'duplicate_customer_id'
)

union all

select 'invalid_email'
where not exists (
    select 1 from {{ ref('qrt_customers') }} where quarantine_reason = 'invalid_email'
)

union all

select 'missing_country'
where not exists (
    select 1 from {{ ref('qrt_customers') }} where quarantine_reason = 'missing_country'
)

union all

select 'duplicate_subscription_id'
where not exists (
    select 1 from {{ ref('qrt_subscriptions') }} where quarantine_reason = 'duplicate_subscription_id'
)

union all

select 'orphan_customer_id'
where not exists (
    select 1 from {{ ref('qrt_subscriptions') }} where quarantine_reason = 'orphan_customer_id'
)

union all

select 'non_positive_monthly_price'
where not exists (
    select 1 from {{ ref('qrt_subscriptions') }} where quarantine_reason = 'non_positive_monthly_price'
)

union all

select 'end_before_start'
where not exists (
    select 1 from {{ ref('qrt_subscriptions') }} where quarantine_reason = 'end_before_start'
)

union all

select 'orphan_subscription_id'
where not exists (
    select 1 from {{ ref('qrt_invoices') }} where quarantine_reason = 'orphan_subscription_id'
)

union all

select 'null_or_non_positive_amount'
where not exists (
    select 1
    from {{ ref('qrt_invoices') }}
    where quarantine_reason in ('null_amount', 'non_positive_amount')
)

union all

select 'invoice_after_end_date'
where not exists (
    select 1 from {{ ref('qrt_invoices') }} where quarantine_reason = 'invoice_after_end_date'
)
