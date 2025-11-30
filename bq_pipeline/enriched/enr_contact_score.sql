WITH
login_30d AS (
  SELECT contact_id, COUNT(*) AS logins_30d
  FROM `k_crm_core.core_fct_login_event`
  WHERE login_time >= TIMESTAMP_SUB('2024-01-01', INTERVAL 30 DAY)
  GROUP BY contact_id

),
usage_30d AS (
  SELECT contact_id, COUNT(*) AS events_30d, COUNT(DISTINCT DATE(event_timestamp)) AS active_days_30d
  FROM `k_crm_core.core_fct_usage_event`
  WHERE event_timestamp >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY contact_id
),
mktg_30d AS (
  SELECT lead_id, COUNT(*) AS touches_30d
  FROM `k_crm_core.core_fct_mktg_touch`
  WHERE touch_time >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY lead_id
),
ticket_30d AS (
  SELECT contact_id, COUNT(*) AS tickets_30d
  FROM `k_crm_core.core_fct_ticket`
  WHERE created_at >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY contact_id
),
nps_contact AS (
  SELECT contact_id, AVG(score) AS avg_nps_contact
  FROM `k_crm_core.core_fct_nps_response`
  GROUP BY contact_id
)
SELECT
  c.contact_id,
  -- engagement: logins and usage normalized
  LEAST(100, ROUND( (COALESCE(l.logins_30d,0)/10)*50 + (COALESCE(u.active_days_30d,0)/30)*50, 2)) AS contact_engagement_score,
  -- lifecycle_score: inferred from lifecycle_stage + conversion signals
  CASE
    WHEN c.lifecycle_stage = 'Lead' THEN 30
    WHEN c.lifecycle_stage = 'MQL' THEN 50
    WHEN c.lifecycle_stage = 'Customer' THEN 80
    ELSE 40
  END AS lifecycle_score,
  -- product activity: usage intensity, feature usage
  LEAST(100, ROUND(COALESCE(u.events_30d,0)/20*100,2)) AS product_activity_score,
  "2024-01-01" AS updated_at
FROM `k_crm_core.core_dim_contact` c
LEFT JOIN login_30d l USING (contact_id)
LEFT JOIN usage_30d u USING (contact_id)
LEFT JOIN ticket_30d t USING (contact_id)
LEFT JOIN nps_contact n USING (contact_id)
;
