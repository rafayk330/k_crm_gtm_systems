WITH recent_churn_events AS (
    SELECT
        account_id,
        COUNT(*) AS churn_events_90d
    FROM `k_crm_core.core_fct_churn_event`
    WHERE event_date >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)
    GROUP BY account_id
),
base AS (
    SELECT
        s.account_id,
        s.churn_risk_score,
        COALESCE(r.churn_events_90d, 0) AS churn_event_count
    FROM `k_crm_enriched.enr_account_score` s
    LEFT JOIN recent_churn_events r
        ON s.account_id = r.account_id
),
risk AS (
    SELECT
        account_id,
        CASE 
            WHEN churn_risk_score >= 70 OR churn_event_count >= 2 THEN 'high'
            WHEN churn_risk_score BETWEEN 40 AND 69 THEN 'medium'
            ELSE 'low'
        END AS risk_bucket
    FROM base
)
SELECT * FROM risk

;
