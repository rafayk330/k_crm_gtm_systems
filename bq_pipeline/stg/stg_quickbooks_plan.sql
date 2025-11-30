SELECT

CAST(plan_id AS STRING) AS plan_id,
CAST(plan_name AS STRING) AS plan_name,
CAST(billing_cycle AS STRING) AS billing_cycle,
CAST(mrr AS FLOAT64) AS mrr

FROM `k_crm_raw.raw_quickbooks_plan`

;
