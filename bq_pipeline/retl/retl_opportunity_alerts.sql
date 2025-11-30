WITH base AS (
    SELECT
        o.oppt_id,
        o.account_id,
        o.stage_id,
        o.amount,
        o.created_at,
        o.closed_at,
        h.health_score
    FROM `k_crm_core.core_fct_oppt` o
    LEFT JOIN `k_crm_enriched.enr_oppt_health` h
        ON o.oppt_id = h.oppt_id
),
alerts AS (
    SELECT
        oppt_id,

        CASE 
            WHEN TIMESTAMP_DIFF("2024-01-01", created_at, DAY) > 45
                 AND closed_at IS NULL
            THEN 'stalling'

            WHEN amount >= 50000
            THEN 'high_value'

            WHEN health_score < 40
            THEN 'close_loss_risk'

            ELSE 'healthy'
        END AS alert_type
    FROM base
)
SELECT * FROM alerts

;
