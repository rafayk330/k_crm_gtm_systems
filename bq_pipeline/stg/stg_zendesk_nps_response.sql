SELECT
  CAST(nps_id AS STRING) AS nps_id,
  CAST(contact_id AS STRING) AS contact_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(score AS INT) AS score,
  CAST(comment AS STRING) AS comment,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', response_timestamp)) AS response_timestamp,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', response_timestamp)) AS response_timestamp_ts
FROM `k_crm_raw.raw_zendesk_nps_response`


;
