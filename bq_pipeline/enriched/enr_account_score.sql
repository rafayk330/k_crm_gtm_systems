WITH
-- 1) Usage signals (30d)
usage_30d AS (
  SELECT
    account_id,
    COUNT(*) AS events_30d,
    COUNT(DISTINCT contact_id) AS active_contacts_30d,
    COUNT(DISTINCT DATE(event_timestamp)) AS active_days_30d
  FROM `k_crm_core.core_fct_usage_event`
  WHERE event_timestamp >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY account_id
)

,

logins_30d AS (
  SELECT
    account_id,
    COUNT(*) AS logins_30d,
    COUNT(DISTINCT contact_id) AS unique_logged_in_contacts
  FROM `k_crm_core.core_fct_login_event`
  WHERE login_time >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY account_id
)

,

-- 3) Support signals (90d)
support_90d AS (
  SELECT
    account_id,
    COUNT(*) AS tickets_90d,
    AVG(TIMESTAMP_DIFF(closed_at, created_at, HOUR)) AS avg_resolve_hours,
    SUM(CASE WHEN ticket_priority = 'high' THEN 1 ELSE 0 END) AS high_priority_tickets
  FROM `k_crm_core.core_fct_ticket`
  WHERE created_at >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)
  GROUP BY account_id
)

,

-- 4) NPS signals (90d)
nps_90d AS (
  SELECT
    account_id,
    COUNT(*) AS nps_count_90d,
    AVG(score) AS avg_nps_90d
  FROM `k_crm_core.core_fct_nps_response`
  WHERE response_timestamp >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)
  GROUP BY account_id
)


,


-- 5) Revenue signals (current active mrr) using subscription + plan
mrr AS (
  SELECT
    s.account_id,
    COALESCE(p.mrr, 0) AS plan_mrr,
    CASE WHEN s.is_active THEN COALESCE(p.mrr,0) ELSE 0 END AS active_mrr
  FROM `k_crm_core.core_dim_subscription` s
  LEFT JOIN `k_crm_core.core_dim_plan` p
    ON s.plan_id = p.plan_id
)

,


mrr_agg AS (
  SELECT
    account_id,
    SUM(active_mrr) AS account_mrr
  FROM mrr
  GROUP BY account_id
)



-- SELECT * FROM mrr_agg

SELECT
  a.account_id,
  COALESCE(m.account_mrr, 0) AS mrr,
  
  -- usage_score: combine events + active days + contacts (normalized)

    LEAST(100, ROUND(
    COALESCE( (COALESCE(u.events_30d,0)/50) * 40 -- depth of usage
            + (COALESCE(u.active_days_30d,0)/30) * 30 -- frequency of usage
            + (COALESCE(u.active_contacts_30d,0)/5) * 30, 0) -- breadth of usage
  ,2)) AS usage_score -- usage_score is defined by the "depth + frequency + breadth of usage"
  ,
  
  
  -- support_interaction_score


  LEAST(100, ROUND(
    COALESCE( 100 - (COALESCE(s.tickets_90d,0) * 4) - (COALESCE(s.high_priority_tickets,0) * 6)
             - (COALESCE(s.avg_resolve_hours,72)/72 * 10), 0)
  ,2)) AS cs_interaction_score,


  -- product_adoption_score

  LEAST(100, ROUND(
    COALESCE( (COALESCE(log.logins_30d,0)/30)*50 + (COALESCE(u.active_days_30d,0)/30)*50, 0)
  ,2)) AS product_adoption_score,


  -- nps influence (scale avg_nps_90d - range [-100,100] -> normalized 0-100)


  CASE
    WHEN n.avg_nps_90d IS NULL THEN NULL
    ELSE ROUND( (n.avg_nps_90d + 100) / 2, 2 )
  END AS nps_normalized,

  -- churn_risk_score

  LEAST(100, ROUND(
    COALESCE(
      40 * (1 - (LEAST(100, (COALESCE(u.active_days_30d,0)/30*100))/100)) -- low active days increases risk
    + 30 * (LEAST(100, COALESCE(s.tickets_90d,0) / 5 * 100)/100) -- more tickets -> more risk
    + 20 * (CASE WHEN n.avg_nps_90d IS NULL THEN 0.5 ELSE (1 - ((n.avg_nps_90d + 100)/200)) END) -- low NPS -> more risk
    + 10 * (CASE WHEN COALESCE(m.account_mrr,0) < 100 THEN 1 ELSE 0 END)
    ,0)
  ,2)) AS churn_risk_score,


  ROUND(
    ( 0.35 * COALESCE(LEAST(100, (COALESCE(u.events_30d,0)/50)*40 + (COALESCE(u.active_days_30d,0)/30)*30),0)
    + 0.25 * COALESCE(LEAST(100, (COALESCE(log.logins_30d,0)/30)*50 + (COALESCE(u.active_days_30d,0)/30)*50),0)
    + 0.20 * COALESCE(LEAST(100, 100 - (COALESCE(s.tickets_90d,0)*4)),0)
    + 0.20 * COALESCE(CASE WHEN n.avg_nps_90d IS NULL THEN 50 ELSE (n.avg_nps_90d + 100)/2 END, 50)
    )/1, 2) AS health_score,
  CURRENT_TIMESTAMP() AS updated_at
FROM `k_crm_core.core_dim_account` a
LEFT JOIN usage_30d u USING (account_id)
LEFT JOIN logins_30d log USING (account_id)
LEFT JOIN support_90d s USING (account_id)
LEFT JOIN nps_90d n USING (account_id)
LEFT JOIN mrr_agg m USING (account_id)
;
