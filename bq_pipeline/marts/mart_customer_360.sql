WITH
-- Core account base
acct AS (
  SELECT
    a.account_id,
    a.account_name,
    a.domain,
    a.industry,
    a.company_size,
    a.crm_owner_id,
    a.lifecycle_stage,
    a.created_at AS account_created_at,
    a.updated_at AS account_updated_at,
    a.geo_id
  FROM k_crm_core.core_dim_account a
),

-- Contact summary: counts and primary contact (first created)
contact_summary AS (
  SELECT
    c.account_id,
    COUNT(1) AS contact_count,
    ARRAY_AGG(c.contact_id ORDER BY c.created_at LIMIT 1)[OFFSET(0)] AS primary_contact_id,
    ARRAY_AGG(c.email ORDER BY c.created_at LIMIT 1)[OFFSET(0)] AS primary_contact_email
  FROM k_crm_core.core_dim_contact c
  GROUP BY c.account_id
),

-- Active subscription/MRR (from enr_subscription_health if present else join plan)
subscription_summary AS (
  SELECT
    s.account_id,
    COUNT(DISTINCT s.subscription_id) AS subscription_count,
    SUM(COALESCE(p.mrr,0) * CASE WHEN s.is_active THEN 1 ELSE 0 END) AS subscription_mrr,
    MIN(s.renewal_date) AS next_renewal_date
  FROM k_crm_core.core_dim_subscription s
  LEFT JOIN k_crm_core.core_dim_plan p ON s.plan_id = p.plan_id
  GROUP BY s.account_id
),

-- Billing: last 30 days revenue (fallback)
billing_30d AS (
  SELECT
    b.account_id,
    SUM(b.amount) AS billed_30d
  FROM k_crm_core.core_fct_billing b
  WHERE b.billing_date >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY b.account_id
),

-- Usage & activity (30d)
usage_30d AS (
  SELECT
    ue.account_id,
    COUNT(1) AS usage_events_30d,
    COUNT(DISTINCT DATE(ue.event_timestamp)) AS usage_active_days_30d
  FROM k_crm_core.core_fct_usage_event ue
  WHERE ue.event_timestamp >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY ue.account_id
),

logins_30d AS (
  SELECT
    l.account_id,
    COUNT(1) AS logins_30d,
    MAX(l.login_time) AS last_login_ts
  FROM k_crm_core.core_fct_login_event l
  WHERE l.login_time >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY l.account_id
),

-- Support summary (90d)
support_90d AS (
  SELECT
    t.account_id,
    COUNT(1) AS tickets_90d,
    SUM(CASE WHEN t.ticket_priority = 'high' THEN 1 ELSE 0 END) AS high_priority_tickets,
    MAX(t.created_at) AS last_ticket_at
  FROM k_crm_core.core_fct_ticket t
  WHERE t.created_at >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)
  GROUP BY t.account_id
),

-- Pipeline summary (BigQuery-friendly: use COUNTIF / SUM(IF(...)))
pipeline_summary AS (
  SELECT
    o.account_id,
    COUNTIF(o.closed_at IS NULL) AS open_opportunities,
    SUM(IF(o.closed_at IS NULL, COALESCE(o.amount,0), 0)) AS open_pipeline_amount,
    COUNTIF(o.closed_at IS NOT NULL AND o.closed_at >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)) AS closed_won_90d
  FROM k_crm_core.core_fct_oppt o
  GROUP BY o.account_id
),

-- Enriched signals: prefer enr tables where present
enr AS (
  SELECT
    ea.account_id,
    ea.mrr,
    ea.usage_score,
    ea.cs_interaction_score,
    ea.product_adoption_score,
    ea.nps_normalized,
    ea.churn_risk_score,
    ea.health_score,
    es.sub_health_score,
    en.latest_nps_score,
    en.nps_90d_avg
  FROM k_crm_enriched.enr_account_score ea
  LEFT JOIN k_crm_enriched.enr_subscription_health es ON ea.account_id = es.account_id
  LEFT JOIN k_crm_enriched.enr_nps_account en ON ea.account_id = en.account_id
)

SELECT
  a.account_id,
  a.account_name,
  a.domain,
  a.industry,
  a.company_size,
  a.crm_owner_id,
  a.lifecycle_stage,
  a.account_created_at,
  a.account_updated_at,
  COALESCE(en.mrr, ss.subscription_mrr, 0) AS mrr,
  COALESCE(en.health_score, NULL) AS health_score,
  COALESCE(en.usage_score, usage_30d.usage_events_30d, 0) AS usage_score_fallback,
  COALESCE(en.product_adoption_score, NULL) AS product_adoption_score,
  COALESCE(en.cs_interaction_score, NULL) AS cs_interaction_score,
  COALESCE(en.nps_normalized, en.latest_nps_score, NULL) AS nps_score,
  cs.contact_count,
  cs.primary_contact_id,
  cs.primary_contact_email,
  COALESCE(ss.subscription_count, 0) AS subscription_count,
  COALESCE(ss.next_renewal_date, NULL) AS next_renewal_date,
  COALESCE(b30.billed_30d, 0) AS billed_30d,
  COALESCE(usage_30d.usage_events_30d, 0) AS usage_events_30d,
  COALESCE(usage_30d.usage_active_days_30d, 0) AS usage_active_days_30d,
  COALESCE(logins_30d.logins_30d, 0) AS logins_30d,
  logins_30d.last_login_ts,
  COALESCE(support_90d.tickets_90d, 0) AS tickets_90d,
  support_90d.high_priority_tickets,
  support_90d.last_ticket_at,
  COALESCE(pipeline_summary.open_opportunities, 0) AS open_opportunities,
  COALESCE(pipeline_summary.open_pipeline_amount, 0) AS open_pipeline_amount,
  COALESCE(pipeline_summary.closed_won_90d, 0) AS closed_won_90d,
  "2024-01-01" AS model_updated_at
FROM acct a
LEFT JOIN contact_summary cs USING(account_id)
LEFT JOIN subscription_summary ss USING(account_id)
LEFT JOIN billing_30d b30 USING(account_id)
LEFT JOIN usage_30d USING(account_id)
LEFT JOIN logins_30d USING(account_id)
LEFT JOIN support_90d USING(account_id)
LEFT JOIN pipeline_summary USING(account_id)
LEFT JOIN enr en USING(account_id)
;
