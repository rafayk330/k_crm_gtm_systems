SELECT
    c360.account_id,
    c360.account_name,
    c360.industry,
    c360.company_size,
    c360.lifecycle_stage,

    -- Financial & product health
    c360.mrr,
    c360.health_score,
    c360.product_adoption_score,
    c360.billed_30d,
    c360.usage_events_30d,
    c360.logins_30d,

    -- Signals
    sig.churn_risk,
    sig.product_adoption_status,
    sig.csm_alert_flag,
    sig.renewal_risk,

    -- Account scores
    es.churn_risk_score,
    es.usage_score,
    es.cs_interaction_score,
    es.nps_normalized,

    -- Expansion cohort info
    exp.delta_billed,
    exp.pct_change AS expansion_pct,

    -- GTM funnel metrics (monthly)
    f.month,
    f.leads,
    f.contacts,
    f.accounts,
    f.activations,
    f.expansions,
    f.churns,
    f.activation_rate_per_account,
    f.expansion_rate_per_account,
    f.churn_rate_per_account

FROM k_crm_marts.mart_customer_360 c360

LEFT JOIN k_crm_enriched.enr_account_score es USING (account_id)
LEFT JOIN k_crm_retl.retl_account_signals sig USING (account_id)
LEFT JOIN k_crm_marts.mart_expansion_cohort exp USING(account_id)
LEFT JOIN k_crm_marts.mart_combined_gtm_funnel f
    ON DATE(DATE_TRUNC(c360.account_created_at, MONTH)) = f.month;
