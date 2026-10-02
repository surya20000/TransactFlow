-- Run these after the pipeline has loaded data into Postgres.


-- Q1: Top-10 accounts by lifetime spend
SELECT
    account_id,
    SUM(total_amount)         AS lifetime_spend,
    SUM(txn_count)            AS lifetime_txns,
    ROUND(AVG(avg_amount), 2) AS avg_daily_avg_amount,
    MAX(spike_count)          AS max_spike_day
FROM account_features
GROUP BY account_id
ORDER BY lifetime_spend DESC
LIMIT 10;


-- Q2: Daily risk ranking with 7-day spike trend
WITH daily_risk AS (
    SELECT
        af.account_id,
        af.date,
        af.spike_count,
        af.max_txn_count_24h,
        df.distinct_accounts AS device_shared_accounts,
        df.multi_account_flag,
        SUM(af.spike_count)
            OVER (PARTITION BY af.account_id
                  ORDER BY af.date
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS spike_7d,
        LAG(af.total_amount) OVER (PARTITION BY af.account_id ORDER BY af.date) AS prev_day_amount
    FROM account_features af
    LEFT JOIN device_features df
           ON df.date = af.date
          AND df.device_id IN (
              SELECT DISTINCT device_id FROM transactions WHERE account_id = af.account_id
          )
)
SELECT
    account_id,
    date,
    spike_count,
    spike_7d,
    device_shared_accounts,
    multi_account_flag,
    ROUND(
        CAST(spike_7d AS NUMERIC) * 0.4
        + CAST(multi_account_flag AS NUMERIC) * 0.3
        + CAST(COALESCE(device_shared_accounts, 1) - 1 AS NUMERIC) * 0.3,
        4
    ) AS composite_risk_score,
    RANK() OVER (PARTITION BY date ORDER BY spike_7d DESC) AS daily_risk_rank
FROM daily_risk
ORDER BY date DESC, daily_risk_rank
LIMIT 50;


-- Q3: Devices shared by more than 2 accounts
SELECT
    df.device_id,
    MAX(df.distinct_accounts) AS max_accounts_on_device,
    SUM(df.txn_count)         AS total_device_txns,
    SUM(df.total_amount)      AS total_device_spend,
    COUNT(DISTINCT df.date)   AS active_days
FROM device_features df
GROUP BY df.device_id
HAVING MAX(df.distinct_accounts) > 2
ORDER BY max_accounts_on_device DESC, total_device_spend DESC
LIMIT 20;


-- Q4: Month-over-month transaction volume
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', timestamp)::DATE AS month,
        COUNT(*)                              AS txn_count,
        SUM(amount)                           AS total_amount,
        COUNT(DISTINCT account_id)            AS active_accounts
    FROM transactions
    GROUP BY 1
)
SELECT
    month,
    txn_count,
    total_amount,
    active_accounts,
    LAG(txn_count)    OVER (ORDER BY month) AS prev_month_txn,
    LAG(total_amount) OVER (ORDER BY month) AS prev_month_amount,
    ROUND(
        100.0 * (txn_count - LAG(txn_count) OVER (ORDER BY month))
              / NULLIF(LAG(txn_count) OVER (ORDER BY month), 0),
        2
    ) AS txn_growth_pct
FROM monthly
ORDER BY month;


-- Q5: Per-account risk bracket using NTILE quartiles
WITH account_totals AS (
    SELECT
        account_id,
        SUM(spike_count)       AS total_spikes,
        SUM(txn_count)         AS total_txns,
        SUM(total_amount)      AS total_spend,
        MAX(max_txn_count_24h) AS peak_24h_txns
    FROM account_features
    GROUP BY account_id
),
percentiles AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY total_spikes DESC)  AS spike_quartile,    -- 1 = most spikes
        NTILE(4) OVER (ORDER BY peak_24h_txns DESC) AS velocity_quartile  -- 1 = highest velocity
    FROM account_totals
)
SELECT
    account_id,
    total_spikes,
    total_txns,
    ROUND(total_spend, 2) AS total_spend,
    peak_24h_txns,
    spike_quartile,
    velocity_quartile,
    CASE
        WHEN spike_quartile = 1 AND velocity_quartile = 1 THEN 'HIGH'
        WHEN spike_quartile <= 2 OR velocity_quartile <= 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_bracket
FROM percentiles
ORDER BY total_spikes DESC, peak_24h_txns DESC
LIMIT 30;


-- EXPLAIN index comparison
-- Step 1 — without index (seq scan baseline):
--   DROP INDEX IF EXISTS idx_transactions_account_id;
--   EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM transactions WHERE account_id = 'ACC000001';
--
-- Step 2 — with index:
--   CREATE INDEX idx_transactions_account_id ON transactions (account_id);
--   EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM transactions WHERE account_id = 'ACC000001';
