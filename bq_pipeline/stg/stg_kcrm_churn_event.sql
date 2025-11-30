SELECT
  CAST(churn_event_id AS STRING) AS churn_event_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(event_date AS TIMESTAMP) AS event_date,
  CAST(reason AS STRING) AS reason
FROM `k_crm_raw.raw_kcrm_churn_event`

;
