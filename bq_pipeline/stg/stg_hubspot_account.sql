SELECT
  CAST(account_id AS STRING) AS account_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(account_name AS STRING) AS account_name,
  CAST(domain AS STRING) AS domain,
  CAST(industry AS STRING) AS industry,
  CAST(company_size AS STRING) AS company_size,
  CAST(crm_owner_id AS STRING) AS crm_owner_id,
  CAST(lifecycle_stage AS STRING) AS lifecycle_stage,
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(updated_at AS TIMESTAMP) AS updated_at
FROM `k_crm_raw.raw_hubspot_account`


;
