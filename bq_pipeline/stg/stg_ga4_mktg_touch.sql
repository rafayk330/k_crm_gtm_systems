SELECT
  CAST(touch_id AS STRING) AS feature_event_id,
  CAST(lead_id AS STRING) AS lead_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(channel AS STRING) AS channel,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M:%s', touch_time)) AS touch_time,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M:%s', touch_time)) AS touch_time_ts
FROM `k_crm_raw.raw_ga4_mktg_touch`

;
