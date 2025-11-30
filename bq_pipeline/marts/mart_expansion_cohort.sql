WITH
-- monthly billed amount per account
billing_monthly AS (
  SELECT
    account_id,
    DATE_TRUNC(DATE(billing_date), MONTH) AS bill_month,
    SUM(amount) AS month_billed
  FROM k_crm_core.core_fct_billing
  GROUP BY account_id, bill_month
),

-- compute month-over-month increase
billing_lag AS (
  SELECT
    b.*,
    LAG(month_billed) OVER (PARTITION BY account_id ORDER BY bill_month) AS prev_month_billed
  FROM billing_monthly b
),

-- expansion events: month_billed > prev_month_billed by at least 10% and delta >= $50
expansion_events AS (
  SELECT
    account_id,
    bill_month AS expansion_month,
    prev_month_billed,
    month_billed AS post_billed,
    month_billed - COALESCE(prev_month_billed, 0) AS delta_billed,
    SAFE_DIVIDE(month_billed - COALESCE(prev_month_billed,0), GREATEST(prev_month_billed,1)) AS pct_change
  FROM billing_lag
  WHERE prev_month_billed IS NOT NULL
    AND month_billed > prev_month_billed * 1.10
    AND month_billed - prev_month_billed >= 50
),

-- alternatively capture new subscription with higher plan_mrr than existing
subscription_events AS (
  SELECT
    s.account_id,
    DATE_TRUNC(DATE(s.renewal_date), MONTH) AS event_month,
    s.subscription_id,
    COALESCE(p.mrr,0) AS plan_mrr
  FROM k_crm_core.core_dim_subscription s
  LEFT JOIN k_crm_core.core_dim_plan p ON s.plan_id = p.plan_id
  WHERE s.is_active = TRUE
)

SELECT
  ee.account_id,
  ee.expansion_month,
  ee.prev_month_billed,
  ee.post_billed,
  ee.delta_billed,
  ROUND(ee.pct_change * 100, 2) AS pct_change,
  'billing_mom' AS expansion_reason,
  "2024-01-01" AS model_updated_at
FROM expansion_events ee

UNION ALL

SELECT
  se.account_id,
  se.event_month AS expansion_month,
  NULL AS prev_month_billed,
  se.plan_mrr AS post_billed,
  se.plan_mrr AS delta_billed,
  NULL AS pct_change,
  'new_subscription' AS expansion_reason,
  "2024-01-01" AS model_updated_at
FROM subscription_events se
-- Optionally dedupe: prefer billing_mom events when both exist for the same month
;
