SELECT
  CAST(contact_id AS STRING) AS contact_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(email AS STRING) AS email,
  CAST(phone AS STRING) AS phone,
  CAST(job_title AS STRING) AS job_title,
  CAST(role AS STRING) AS role,
  CAST(lifecycle_stage AS STRING) AS lifecycle_stage,
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(updated_at AS TIMESTAMP) AS updated_at
FROM `k_crm_raw.raw_hubspot_contact`


;
