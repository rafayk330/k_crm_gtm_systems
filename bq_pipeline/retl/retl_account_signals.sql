WITH base AS (
    SELECT
        s.account_id,

        -- From enr_account_score
        s.health_score,
        s.usage_score,
        s.cs_interaction_score,
        s.product_adoption_score,
        s.churn_risk_score,

        -- From enr_subscription_health
        sh.sub_health_score,

        -- From enr_product_adoption
        pa.adoption_score
    FROM `k_crm_enriched.enr_account_score` s
    LEFT JOIN `k_crm_enriched.enr_subscription_health` sh 
        ON s.account_id = sh.account_id
    LEFT JOIN `k_crm_enriched.enr_product_adoption` pa
        ON s.account_id = pa.product_id    -- (if adoption is product-level)
),
signals AS (
    SELECT
        account_id,

        -- Churn Risk Categorization
        CASE 
            WHEN churn_risk_score >= 70 THEN 'high'
            WHEN churn_risk_score BETWEEN 30 AND 69 THEN 'medium'
            ELSE 'low'
        END AS churn_risk,

        -- Product Adoption Bands
        CASE 
            WHEN product_adoption_score >= 70 THEN 'strong'
            WHEN product_adoption_score BETWEEN 30 AND 69 THEN 'average'
            ELSE 'poor'
        END AS product_adoption_status,

        -- A simple CSM alert rule
        CASE 
            WHEN health_score < 40 OR churn_risk_score >= 70 THEN 1
            ELSE 0
        END AS csm_alert_flag,

        -- Renewal risk heuristic
        CASE
            WHEN sub_health_score < 40 THEN 'high'
            WHEN sub_health_score BETWEEN 40 AND 69 THEN 'medium'
            ELSE 'low'
        END AS renewal_risk
    FROM base
)
SELECT * FROM signals

;
