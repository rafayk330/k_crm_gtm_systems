SELECT

CAST(billing_id AS STRING) AS billing_id,
CAST(account_id AS STRING) AS account_id,
CAST(subscription_id AS STRING) AS subscription_id,
CAST(geo_id AS STRING) AS geo_id,
CAST(amount AS FLOAT64) AS amount,
CAST(billing_date AS TIMESTAMP) AS billing_date

FROM `k_crm_raw.raw_quickbooks_billing`

;
