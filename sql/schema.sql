-- psql -U transactflow -d transactflow -f schema.sql
-- All CREATE statements use IF NOT EXISTS so reruns are safe.

-- OLTP: one row per transaction, source of truth
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  UUID          PRIMARY KEY,
    account_id      VARCHAR(12)   NOT NULL,
    device_id       VARCHAR(12)   NOT NULL,
    amount          NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    merchant        VARCHAR(10)   NOT NULL,
    timestamp       TIMESTAMPTZ   NOT NULL,
    city            VARCHAR(64)   NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- OLAP: pre-aggregated daily features consumed by the API (no joins at read time)
CREATE TABLE IF NOT EXISTS account_features (
    account_id         VARCHAR(12)   NOT NULL,
    date               DATE          NOT NULL,
    txn_count          INT           NOT NULL DEFAULT 0,
    total_amount       NUMERIC(18,2) NOT NULL DEFAULT 0,
    avg_amount         NUMERIC(14,4) NOT NULL DEFAULT 0,
    max_txn_count_24h  INT           NOT NULL DEFAULT 0,
    spike_count        INT           NOT NULL DEFAULT 0,
    distinct_merchants INT           NOT NULL DEFAULT 0,
    distinct_cities    INT           NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, date)
);

CREATE TABLE IF NOT EXISTS device_features (
    device_id          VARCHAR(12)   NOT NULL,
    date               DATE          NOT NULL,
    distinct_accounts  INT           NOT NULL DEFAULT 0,
    txn_count          INT           NOT NULL DEFAULT 0,
    total_amount       NUMERIC(18,2) NOT NULL DEFAULT 0,
    multi_account_flag SMALLINT      NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, date)
);

CREATE INDEX IF NOT EXISTS idx_transactions_account_id  ON transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp   ON transactions (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_device_id   ON transactions (device_id);
CREATE INDEX IF NOT EXISTS idx_account_features_date    ON account_features (date);
CREATE INDEX IF NOT EXISTS idx_device_features_date     ON device_features (date);
