SELECT
  CAST(user_id AS STRING) AS user_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(full_name AS STRING) AS full_name,
  CAST(email AS STRING) AS email,
  CAST(team AS STRING) AS team,
  CAST(role AS STRING) AS role,

FROM `k_crm_raw.raw_hubspot_user`



;
