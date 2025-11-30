SELECT
  CAST(feature_event_id AS STRING) AS feature_event_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(feature_name AS STRING) AS feature_name,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M', used_at) AS used_at,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M', used_at) AS used_at_ts
FROM
  `k-crm-478902.k_crm_raw.raw_mixpanel_feature_adoption`;
