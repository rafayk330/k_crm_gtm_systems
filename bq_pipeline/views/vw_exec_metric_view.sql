WITH base_funnel AS (
    -- Your existing CTE: R_F rows
    SELECT
        month,
        leads,
        contacts,
        accounts,
        activations,
        expansions,
        churns,
        activation_rate_per_account,
        expansion_rate_per_account,
        churn_rate_per_account
    FROM k_crm_marts.mart_combined_gtm_funnel
),

global_nps_avg AS (
    -- Calculate the single, global AVG NPS
    SELECT
        AVG(latest_nps_score) AS global_avg_nps -- Changed to latest_nps_score as it's typically the field of interest, but use nps_90d_avg if intended
    FROM k_crm_enriched.enr_nps_account
),

global_usage AS (
    -- Calculate the single, global SUM of product usage
    SELECT
        SUM(total_uses_30d) AS global_usage_30d
    FROM k_crm_enriched.enr_product_adoption
),

global_churn_count AS (
    -- Calculate the single, global count of churned accounts
    SELECT
        COUNTIF(churn_label = 1) AS total_churned_accounts_90d
    FROM k_crm_marts.mart_churn_labels
)

SELECT
    f.*,
    n.global_avg_nps,
    u.global_usage_30d,
    c.total_churned_accounts_90d
FROM base_funnel f
CROSS JOIN global_nps_avg n      -- Joining a 1-row result set to R_F rows is efficient
CROSS JOIN global_usage u
CROSS JOIN global_churn_count c
;
