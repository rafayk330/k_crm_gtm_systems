WITH billing AS (
    SELECT
        DATE(billing_date) AS bill_date,
        account_id,
        SUM(amount) AS billed_amount
    FROM k_crm_core.core_fct_billing
    GROUP BY 1, 2
),

expansion AS (
    SELECT
        account_id,
        expansion_month,
        delta_billed,
        pct_change,
        expansion_reason
    FROM k_crm_marts.mart_expansion_cohort
),

sub_health AS (
    SELECT
        account_id,
        sub_health_score,
        days_to_renewal
    FROM k_crm_enriched.enr_subscription_health
),

account_scores AS (
    SELECT
        account_id,
        mrr,
        health_score,
        churn_risk_score
    FROM k_crm_enriched.enr_account_score
)

SELECT
    b.bill_date,
    b.account_id,
    b.billed_amount,
    e.delta_billed,
    e.pct_change,
    e.expansion_reason,
    s.sub_health_score,
    s.days_to_renewal,
    a.mrr,
    a.health_score,
    a.churn_risk_score
FROM billing b
LEFT JOIN expansion e USING(account_id)
LEFT JOIN sub_health s USING(account_id)
LEFT JOIN account_scores a USING(account_id)
ORDER BY bill_date DESC;
