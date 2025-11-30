SELECT

CAST(subscription_id AS STRING) AS subscription_id,
CAST(account_id AS STRING) AS account_id,
CAST(geo_id AS STRING) AS geo_id,
CAST(plan_id AS STRING) AS plan_id,
CAST(renewal_date AS TIMESTAMP) AS renewal_date,
CAST(is_active AS BOOLEAN) AS is_active

FROM `k_crm_raw.raw_quickbooks_subscription`

;
