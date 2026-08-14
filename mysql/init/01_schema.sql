-- Raw billing tables (grain matches seed_data CSVs). Loaded by scripts/seed_mysql.py.
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id   VARCHAR(32)  NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    email         VARCHAR(255) NOT NULL,
    country       VARCHAR(16)  NULL,
    created_at    DATE         NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_subscriptions (
    subscription_id VARCHAR(32)    NOT NULL,
    customer_id     VARCHAR(32)    NOT NULL,
    plan_name       VARCHAR(64)    NOT NULL,
    monthly_price   DECIMAL(12, 2) NOT NULL,
    start_date      DATE           NOT NULL,
    end_date        DATE           NULL,
    status          VARCHAR(32)    NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_invoices (
    invoice_id      VARCHAR(32)    NOT NULL,
    subscription_id VARCHAR(32)    NOT NULL,
    invoice_date    DATE           NOT NULL,
    amount          DECIMAL(12, 2) NULL,
    currency        VARCHAR(8)     NOT NULL,
    status          VARCHAR(32)    NOT NULL
);
