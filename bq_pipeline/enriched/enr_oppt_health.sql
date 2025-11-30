WITH
oppt_stage_days AS (
  SELECT
    oppt_id,
    account_id,
    stage_id,
    owner_user_id,
    amount,
    created_at,
    closed_at,
    DATE_DIFF("2024-01-01", DATE(created_at), DAY) AS days_since_create
  FROM `k_crm_core.core_fct_oppt`
),
usage_account_30d AS (
  SELECT account_id, COUNT(*) AS events_30d
  FROM `k_crm_core.core_fct_usage_event`
  WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY account_id
),
account_health AS (
  SELECT account_id, health_score FROM `k_crm_enriched.enr_account_score`
)

SELECT
  o.oppt_id,
  o.account_id,
  o.stage_id,
  o.owner_user_id,
  o.amount,
  -- stage risk: if in late stages but > 30 days since creation -> risk
  CASE
    WHEN o.days_since_create > 60 THEN 50
    WHEN o.days_since_create > 30 THEN 30
    ELSE 10
  END AS time_in_stage_risk,
  -- engagement component: low usage on account increases risk
  CASE WHEN COALESCE(u.events_30d,0) = 0 THEN 40 ELSE 0 END AS engagement_risk,
  -- account health factor reduces overall risk; invert health_score to risk contribution
  CASE WHEN a.health_score IS NULL THEN 30 ELSE (100 - a.health_score) * 0.2 END AS account_health_risk,
  -- combined health_score (lower is better for opp health)
  ROUND(100 - LEAST(100, GREATEST(0,
    100 - ( (100 - (CASE WHEN o.days_since_create > 60 THEN 50 WHEN o.days_since_create > 30 THEN 30 ELSE 10 END))
          + (CASE WHEN COALESCE(u.events_30d,0) = 0 THEN -40 ELSE 0 END)
          + (CASE WHEN a.health_score IS NULL THEN 0 ELSE a.health_score * 0.2 END)
    ) )), 2) AS health_score,
  CURRENT_TIMESTAMP() AS updated_at
FROM oppt_stage_days o
LEFT JOIN usage_account_30d u ON o.account_id = u.account_id
LEFT JOIN account_health a ON o.account_id = a.account_id
;
