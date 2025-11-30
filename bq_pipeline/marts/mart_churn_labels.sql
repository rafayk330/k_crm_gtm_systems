WITH
-- last activity timestamps
last_usage AS (
  SELECT
    account_id,
    MAX(event_timestamp) AS last_usage_ts
  FROM k_crm_core.core_fct_usage_event
  GROUP BY account_id
),

last_login AS (
  SELECT
    account_id,
    MAX(login_time) AS last_login_ts
  FROM k_crm_core.core_fct_login_event
  GROUP BY account_id
),

-- subscription status (latest)
subs AS (
  SELECT
    subscription_id,
    account_id,
    is_active,
    renewal_date
  FROM k_crm_core.core_dim_subscription
),

-- churn event records (explicit cancellations)
churn_events AS (
  SELECT 
    account_id, 
    MIN(event_date) AS churn_event_date   -- DATE
  FROM k_crm_core.core_fct_churn_event
  GROUP BY account_id
),

account_activity AS (
  SELECT
    a.account_id,
    COALESCE(lu.last_usage_ts, TIMESTAMP '1970-01-01') AS last_usage_ts,
    COALESCE(ll.last_login_ts, TIMESTAMP '1970-01-01') AS last_login_ts,
    CASE 
      WHEN (
        COALESCE(lu.last_usage_ts, TIMESTAMP '1970-01-01') < TIMESTAMP_SUB(TIMESTAMP("2024-01-01 00:01:00 UTC"), INTERVAL 90 DAY)
        AND COALESCE(ll.last_login_ts, TIMESTAMP '1970-01-01') < TIMESTAMP_SUB(TIMESTAMP("2024-01-01 00:01:00 UTC"), INTERVAL 90 DAY)
      )
      THEN TRUE 
      ELSE FALSE 
    END AS inactivity_90d
  FROM k_crm_core.core_dim_account a
  LEFT JOIN last_usage lu USING(account_id)
  LEFT JOIN last_login ll USING(account_id)
),

-- subscription-derived churn
subs_status AS (
  SELECT
    s.account_id,
    MAX(CASE WHEN s.is_active = FALSE THEN 1 ELSE 0 END) AS has_inactive_subscription
  FROM subs s
  GROUP BY s.account_id
)

SELECT
  a.account_id,

  -- churn_label
  CASE
    WHEN ce.churn_event_date IS NOT NULL THEN 1
    WHEN ss.has_inactive_subscription = 1 THEN 1
    WHEN aa.inactivity_90d = TRUE THEN 1
    ELSE 0
  END AS churn_label,

  -- churn_date as TIMESTAMP (Option B)
  COALESCE(
    TIMESTAMP(ce.churn_event_date),
    CASE WHEN ss.has_inactive_subscription = 1 THEN TIMESTAMP("2024-01-01 00:01:00 UTC") END
  ) AS churn_date,

  aa.last_usage_ts,
  aa.last_login_ts,
  aa.inactivity_90d,
  ss.has_inactive_subscription,

  -- enriched score (unchanged)
  COALESCE(ea.churn_risk_score, NULL) AS churn_risk_score,

  TIMESTAMP("2024-01-01 00:01:00 UTC") AS model_updated_at

FROM k_crm_core.core_dim_account a
LEFT JOIN churn_events ce USING(account_id)
LEFT JOIN account_activity aa USING(account_id)
LEFT JOIN subs_status ss USING(account_id)
LEFT JOIN k_crm_enriched.enr_account_score ea USING(account_id);
