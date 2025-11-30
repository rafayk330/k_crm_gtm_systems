SELECT
  CAST(login_event_id AS STRING) AS login_event_id,
  CAST(contact_id AS STRING) AS contact_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(geo_id AS STRING) AS geo_id,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M', login_time)) AS login_time,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M', login_time)) AS login_time_ts
FROM `k_crm_raw.raw_mixpanel_login_event`

;
