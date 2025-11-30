WITH
-- base leads by month
leads_base AS (
  SELECT
    l.lead_id,
    DATE_TRUNC(DATE(l.created_at), MONTH) AS lead_month,
    l.is_converted,
    l.converted_account_id
  FROM k_crm_core.core_dim_lead l
),

-- contacts mapped to accounts
contacts_base AS (
  SELECT
    c.contact_id,
    c.account_id,
    DATE_TRUNC(DATE(c.created_at), MONTH) AS contact_month
  FROM k_crm_core.core_dim_contact c
),

-- accounts creation month
accounts_base AS (
  SELECT
    a.account_id,
    DATE_TRUNC(DATE(a.created_at), MONTH) AS account_month
  FROM k_crm_core.core_dim_account a
),

-- activations from activation cohort
activations AS (
  SELECT
    account_id,
    DATE(activation_date) AS activation_date,
    DATE_TRUNC(DATE(activation_date), MONTH) AS activation_month
  FROM k_crm_marts.mart_activation_cohort
  WHERE activated = TRUE
),

-- expansions
expansions AS (
  SELECT
    account_id,
    expansion_month
  FROM k_crm_marts.mart_expansion_cohort
),

-- churn events
churns AS (
  SELECT 
    account_id, 
    churn_label, 
    churn_date
  FROM k_crm_marts.mart_churn_labels
  WHERE churn_label = 1
),

-- monthly aggregates
lead_counts AS (
  SELECT lead_month AS month, COUNT(lead_id) AS leads
  FROM leads_base
  GROUP BY lead_month
),

contact_counts AS (
  SELECT contact_month AS month, COUNT(contact_id) AS contacts
  FROM contacts_base
  GROUP BY contact_month
),

account_counts AS (
  SELECT account_month AS month, COUNT(account_id) AS accounts
  FROM accounts_base
  GROUP BY account_month
),

activation_counts AS (
  SELECT activation_month AS month, COUNT(DISTINCT account_id) AS activations
  FROM activations
  GROUP BY activation_month
),

expansion_counts AS (
  SELECT expansion_month AS month, COUNT(DISTINCT account_id) AS expansions
  FROM expansions
  GROUP BY expansion_month
),

churn_counts AS (
  SELECT 
    DATE_TRUNC(DATE(churn_date), MONTH) AS month,
    COUNT(DISTINCT account_id) AS churns
  FROM churns
  GROUP BY month
)

SELECT
  COALESCE(lc.month, cc.month, ac.month, act.month, ex.month, ch.month) AS month,
  COALESCE(lc.leads, 0) AS leads,
  COALESCE(cc.contacts, 0) AS contacts,
  COALESCE(ac.accounts, 0) AS accounts,
  COALESCE(act.activations, 0) AS activations,
  COALESCE(ex.expansions, 0) AS expansions,
  COALESCE(ch.churns, 0) AS churns,

  -- conversion funnel metrics
  SAFE_DIVIDE(COALESCE(cc.contacts,0), NULLIF(COALESCE(lc.leads,0),0)) AS contacts_per_lead,
  SAFE_DIVIDE(COALESCE(ac.accounts,0), NULLIF(COALESCE(cc.contacts,0),0)) AS accounts_per_contact,
  SAFE_DIVIDE(COALESCE(act.activations,0), NULLIF(COALESCE(ac.accounts,0),0)) AS activation_rate_per_account,
  SAFE_DIVIDE(COALESCE(ex.expansions,0), NULLIF(COALESCE(ac.accounts,0),0)) AS expansion_rate_per_account,
  SAFE_DIVIDE(COALESCE(ch.churns,0), NULLIF(COALESCE(ac.accounts,0),0)) AS churn_rate_per_account,

  "2024-01-01" AS model_updated_at

FROM lead_counts lc
FULL OUTER JOIN contact_counts cc ON lc.month = cc.month
FULL OUTER JOIN account_counts ac ON COALESCE(lc.month, cc.month) = ac.month
FULL OUTER JOIN activation_counts act ON COALESCE(lc.month, cc.month, ac.month) = act.month
FULL OUTER JOIN expansion_counts ex ON COALESCE(lc.month, cc.month, ac.month, act.month) = ex.month
FULL OUTER JOIN churn_counts ch ON COALESCE(lc.month, cc.month, ac.month, act.month, ex.month) = ch.month
ORDER BY month DESC;
