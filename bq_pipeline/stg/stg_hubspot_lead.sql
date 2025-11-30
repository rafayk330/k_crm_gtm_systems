SELECT
  CAST(lead_id AS STRING) AS lead_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(email AS STRING) AS email,
  CAST(`source` AS STRING) AS `source`,
  CAST(utm_campaign AS STRING) AS utm_campaign,
  CAST(utm_medium AS STRING) AS utm_medium,
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(updated_at AS TIMESTAMP) AS updated_at,
  CAST(is_converted AS BOOLEAN) AS is_converted,
  CAST(converted_account_id AS STRING) AS converted_account_id,

FROM `k_crm_raw.raw_hubspot_lead`


;
