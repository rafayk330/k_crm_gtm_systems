WITH base AS (
    SELECT
        l.lead_id,
        l.is_converted,
        l.source,
        l.utm_medium,
        es.behavioral_score,
        es.demographic_score,
        es.total_score
    FROM `k_crm_enriched.enr_lead_score` es
    LEFT JOIN `k_crm_core.core_dim_lead` l 
        ON es.lead_id = l.lead_id
),
activation AS (
    SELECT
        lead_id,
        total_score AS lead_score,

        CASE 
            WHEN total_score >= 75 THEN 'hot'
            WHEN total_score BETWEEN 40 AND 74 THEN 'warm'
            ELSE 'cold'
        END AS activation_status,

        CASE 
            WHEN behavioral_score >= 60 
                 AND demographic_score >= 50
            THEN 1 ELSE 0
        END AS pql_flag
    FROM base
)
SELECT * FROM activation

;
