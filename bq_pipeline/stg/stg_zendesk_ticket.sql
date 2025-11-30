SELECT
  CAST(ticket_id AS STRING) AS ticket_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(contact_id AS STRING) AS contact_id,
  CAST(agent_user_id AS STRING) AS agent_user_id,
  CAST(geo_id AS STRING) AS geo_id,
  CAST(ticket_status AS STRING) AS ticket_status,
  CAST(ticket_priority AS STRING) AS ticket_priority,
  CAST(ticket_channel AS STRING) AS ticket_channel,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at)) AS created_at,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', closed_at)) AS closed_at,
  TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at)) AS created_at_ts
FROM `k_crm_raw.raw_zendesk_ticket`


;
