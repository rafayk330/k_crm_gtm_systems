WITH funnel AS (
    SELECT
        month,
        leads,
        contacts,
        accounts,
        activations,
        activation_rate_per_account
    FROM k_crm_marts.mart_combined_gtm_funnel
),

cohorts AS (
    SELECT
        cohort_month AS month,
        COUNT(DISTINCT account_id) AS cohort_accounts,
        -- FIX: CAST the boolean 'activated' to INT64 (TRUE=1, FALSE=0) so SUM can count it.
        SUM(CAST(activated AS INT64)) AS activated_accounts,
        AVG(days_to_first_event) AS avg_days_to_activation
    FROM k_crm_marts.mart_activation_cohort
    GROUP BY 1
)

SELECT
    f.*,
    c.cohort_accounts,
    c.activated_accounts,
    c.avg_days_to_activation,
    SAFE_DIVIDE(c.activated_accounts, c.cohort_accounts) AS cohort_activation_rate
FROM funnel f
LEFT JOIN cohorts c USING(month)
ORDER BY month;
