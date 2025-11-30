SELECT
  CAST(oppt_id AS STRING) AS oppt_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(stage_id AS STRING) AS stage_id,
  CAST(owner_user_id AS STRING) AS owner_user_id,
  CAST(amount AS FLOAT64) AS amount,
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(closed_at AS TIMESTAMP) AS closed_at,

FROM `k_crm_raw.raw_hubspot_opportunity`


;
