WITH
-- determine first 30-day window after account creation
account_windows AS (
  SELECT
    account_id,
    DATE(account_created_at) AS account_date,
    TIMESTAMP(account_created_at) AS acct_created_ts,
    TIMESTAMP_ADD(TIMESTAMP(account_created_at), INTERVAL 30 DAY) AS activation_deadline_ts,
    DATE_TRUNC(DATE(account_created_at), MONTH) AS cohort_month
  FROM (
    SELECT account_id, created_at AS account_created_at
    FROM k_crm_core.core_dim_account
  )
),

usage_in_window AS (
  SELECT
    aw.account_id,
    COUNT(*) AS usage_events_in_window,
    COUNT(DISTINCT DATE(ue.event_timestamp)) AS active_days_in_window,
    MIN(ue.event_timestamp) AS first_event_ts
  FROM account_windows aw
  LEFT JOIN k_crm_core.core_fct_usage_event ue
    ON aw.account_id = ue.account_id
    AND ue.event_timestamp BETWEEN aw.acct_created_ts AND aw.activation_deadline_ts
  GROUP BY aw.account_id
),

activation_flags AS (
  SELECT
    aw.account_id,
    aw.cohort_month,
    COALESCE(ui.usage_events_in_window,0) AS events_30d,
    COALESCE(ui.active_days_in_window,0) AS active_days_30d,
    CASE
      WHEN COALESCE(ui.active_days_in_window,0) >= 3 THEN TRUE
      WHEN COALESCE(ui.usage_events_in_window,0) >= 10 THEN TRUE
      ELSE FALSE
    END AS activated,
    ui.first_event_ts,
    DATE_DIFF(DATE(ui.first_event_ts), DATE(aw.acct_created_ts), DAY) AS days_to_first_event,
    CASE
      WHEN (CASE WHEN COALESCE(ui.active_days_in_window,0) >= 3 THEN TRUE
                 WHEN COALESCE(ui.usage_events_in_window,0) >= 10 THEN TRUE
                 ELSE FALSE END) THEN
        DATE(ui.first_event_ts)
      ELSE NULL
    END AS activation_date
  FROM account_windows aw
  LEFT JOIN usage_in_window ui USING(account_id)
)

SELECT
  account_id,
  cohort_month,
  events_30d,
  active_days_30d,
  activated,
  activation_date,
  days_to_first_event,
  '2024-01-01' AS model_updated_at -- should ideally be CURRENT_TIMESTAMP() instead of '2024-01-01'
FROM activation_flags



;
