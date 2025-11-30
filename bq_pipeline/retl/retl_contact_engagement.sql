WITH usage_metrics AS (
    SELECT
        contact_id,
        COUNT(*) AS login_count_30d
    FROM `k_crm_core.core_fct_login_event`
    WHERE login_time >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
    GROUP BY contact_id
),
feature_metrics AS (
    SELECT
        account_id, --should be contact_id ideally
        COUNT(*) AS feature_events_30d
    FROM `k_crm_core.core_fct_feature_adoption`
    WHERE used_at >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
    GROUP BY account_id
),
base AS (
    SELECT
        c.contact_id,
        c.account_id,
        s.product_activity_score,
        s.contact_engagement_score,
        s.lifecycle_score,
        COALESCE(u.login_count_30d, 0) AS login_count,
        COALESCE(f.feature_events_30d, 0) AS feature_count
    FROM `k_crm_enriched.enr_contact_score` s
    LEFT JOIN `k_crm_core.core_dim_contact` c
        ON s.contact_id = c.contact_id
    LEFT JOIN usage_metrics u
        ON c.contact_id = u.contact_id
    LEFT JOIN feature_metrics f
        ON c.account_id = f.account_id --should be contact_id here in both!
),
eng AS (
    SELECT
        contact_id,

        -- Engagement Level
        CASE 
            WHEN login_count >= 10 OR feature_count >= 20 THEN 'high'
            WHEN login_count BETWEEN 3 AND 9 THEN 'medium'
            ELSE 'low'
        END AS engagement_level,

        -- Recommended action
        CASE
            WHEN product_activity_score < 30 THEN 'email'
            WHEN login_count < 3 THEN 'call'
            ELSE 'demo'
        END AS next_best_action
    FROM base
)
SELECT * FROM eng

;
;;
