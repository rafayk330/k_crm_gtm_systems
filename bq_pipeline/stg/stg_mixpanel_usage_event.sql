SELECT
  CAST(event_id AS STRING) AS event_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(contact_id AS STRING) AS contact_id,
  CAST(subscription_id AS STRING) AS subscription_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(event_type AS STRING) AS event_type,
  CAST(event_name AS STRING) AS event_name,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M', event_timestamp)) AS event_timestamp,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M', event_timestamp)) AS event_timestamp_ts
FROM `k_crm_raw.raw_mixpanel_usage_event`

;
