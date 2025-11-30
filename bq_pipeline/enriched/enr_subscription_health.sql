WITH
last_billing AS (
  SELECT
    subscription_id,
    MAX(billing_date) AS last_billing_date,
    SUM(amount) AS billed_amount_90d
  FROM `k_crm_core.core_fct_billing`
  WHERE billing_date >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)
  GROUP BY subscription_id
),
recent_churn AS (
  SELECT account_id, COUNT(*) AS churn_events_last_90d ----NEED TO HAVE SUBSCRIPTION_ID IN THE DATASET
  FROM `k_crm_core.core_fct_churn_event`
  WHERE event_date >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)
  GROUP BY account_id --NEED TO HAVE SUBSCRIPTION_ID IN THE DATASET
)
SELECT
  s.subscription_id,
  s.account_id,
  s.plan_id,
  s.is_active,
  COALESCE(p.mrr, 0) AS plan_mrr,
  COALESCE(lb.last_billing_date, NULL) AS last_billing_date,
  COALESCE(lb.billed_amount_90d, 0) AS billed_amount_90d,
  COALESCE(rc.churn_events_last_90d, 0) AS churn_events_last_90d,
  -- time to renewal in days
  DATE_DIFF(DATE(s.renewal_date), CURRENT_DATE(), DAY) AS days_to_renewal,
  -- sub_health_score: start at 100, deduct for churn events, missing billing, low usage implied by billed_amount
  ROUND(
    LEAST(100, GREATEST(0,
      100
      - (COALESCE(rc.churn_events_last_90d,0) * 50)
      - (CASE WHEN COALESCE(lb.last_billing_date, TIMESTAMP '1970-01-01') < TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY) THEN 30 ELSE 0 END)
      - (CASE WHEN COALESCE(p.mrr,0) < 50 AND COALESCE(lb.billed_amount_90d,0) = 0 THEN 10 ELSE 0 END)
      - (CASE WHEN DATE_DIFF(DATE(s.renewal_date), CURRENT_DATE(), DAY) <= 0 THEN 50 ELSE 0 END)
    )), 2) AS sub_health_score,
  "2024-01-01" AS updated_at
FROM `k_crm_core.core_dim_subscription` s
LEFT JOIN `k_crm_core.core_dim_plan` p ON s.plan_id = p.plan_id
LEFT JOIN last_billing lb USING (subscription_id)
LEFT JOIN recent_churn rc USING (account_id) -- SHOULD IDEALLY BE SUBSCRIPTION_ID HERE!

;
