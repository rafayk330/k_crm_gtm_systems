WITH
touch_30d AS (
  SELECT lead_id, COUNT(*) AS touches_30d
  FROM `k_crm_core.core_fct_mktg_touch`
  WHERE touch_time >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY lead_id
),
lead_conversion AS (
  SELECT lead_id, converted_account_id
  FROM `k_crm_core.core_dim_lead`
  WHERE is_converted = TRUE
),
lead_behaviour AS (
  SELECT
    l.lead_id,
    COALESCE(t.touches_30d,0) AS touches_30d
  FROM `k_crm_core.core_dim_lead` l
  LEFT JOIN touch_30d t USING (lead_id)
)
SELECT
  lb.lead_id,
  -- behavioral score: touch frequency
  LEAST(100, ROUND( (COALESCE(lb.touches_30d,0)/5)*100, 2)) AS behavioral_score,
  -- demographic score: simple signals from source & utm
  CASE
    WHEN l.source = 'Referral' THEN 80
    WHEN l.utm_medium = 'cpc' THEN 40
    WHEN l.utm_medium = 'organic' THEN 60
    ELSE 30
  END AS demographic_score,
  -- total_score = weighted avg (behavior 60% + demo 40%)
  ROUND( 0.6 * LEAST(100, (COALESCE(lb.touches_30d,0)/5)*100) + 0.4 * CASE
    WHEN l.source = 'Referral' THEN 80
    WHEN l.utm_medium = 'cpc' THEN 40
    WHEN l.utm_medium = 'organic' THEN 60
    ELSE 30 END, 2) AS total_score,
  "2024-01-01" AS updated_at
FROM lead_behaviour lb
JOIN `k_crm_core.core_dim_lead` l USING (lead_id)
;
