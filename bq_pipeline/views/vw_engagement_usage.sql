WITH logins AS (
    SELECT
        DATE(login_time) AS activity_date,
        account_id,
        COUNT(*) AS logins
    FROM k_crm_core.core_fct_login_event
    GROUP BY 1, 2
),

usage_events AS (
    SELECT
        DATE(event_timestamp) AS activity_date,
        account_id,
        COUNT(*) AS usage_events
    FROM k_crm_core.core_fct_usage_event
    GROUP BY 1, 2
),

feature_events AS (
    SELECT
        DATE(used_at) AS activity_date,
        account_id,
        COUNT(*) AS feature_uses
    FROM k_crm_core.core_fct_feature_adoption
    GROUP BY 1, 2
),

contact_scores AS (
    SELECT
        contact_id,
        contact_engagement_score
    FROM k_crm_enriched.enr_contact_score
)

SELECT
    COALESCE(l.activity_date, u.activity_date, f.activity_date) AS activity_date,
    COALESCE(l.account_id, u.account_id, f.account_id) AS account_id,
    l.logins,
    u.usage_events,
    f.feature_uses,
    AVG(cs.contact_engagement_score) OVER() AS avg_engagement_score
FROM logins l
FULL JOIN usage_events u
  ON l.activity_date = u.activity_date AND l.account_id = u.account_id
FULL JOIN feature_events f
  ON l.activity_date = f.activity_date AND l.account_id = f.account_id
LEFT JOIN k_crm_core.core_dim_contact c
  ON c.account_id = COALESCE(l.account_id, u.account_id, f.account_id)
LEFT JOIN contact_scores cs
  ON c.contact_id = cs.contact_id;
