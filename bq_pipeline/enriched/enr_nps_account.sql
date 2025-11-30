WITH
all_nps AS (
  SELECT
    account_id,
    score,
    response_timestamp,
    CASE
      WHEN score >= 9 THEN 'promoter'
      WHEN score >= 7 THEN 'passive'
      ELSE 'detractor'
    END AS bucket
  FROM `k_crm_core.core_fct_nps_response`
),
latest_per_account AS (
  SELECT account_id, ARRAY_AGG(score ORDER BY response_timestamp DESC LIMIT 1)[OFFSET(0)] AS latest_nps_score,
         MAX(response_timestamp) AS latest_response_ts
  FROM all_nps
  GROUP BY account_id
),
agg_90d AS (
  SELECT account_id,
         AVG(score) AS nps_90d_avg,
         SUM(CASE WHEN bucket='promoter' THEN 1 ELSE 0 END) AS promoter_count,
         SUM(CASE WHEN bucket='detractor' THEN 1 ELSE 0 END) AS detractor_count,
         SUM(CASE WHEN bucket='passive' THEN 1 ELSE 0 END) AS passive_count
  FROM all_nps
  WHERE response_timestamp >= TIMESTAMP_SUB("2024-01-01", INTERVAL 90 DAY)
  GROUP BY account_id
)
SELECT
  a.account_id,
  COALESCE(l.latest_nps_score, NULL) AS latest_nps_score,
  COALESCE(agg.nps_90d_avg, NULL) AS nps_90d_avg,
  COALESCE(agg.promoter_count,0) AS promoter_count,
  COALESCE(agg.detractor_count,0) AS detractor_count,
  COALESCE(agg.passive_count,0) AS passive_count,
  "2024-01-01" AS updated_at
FROM `k_crm_core.core_dim_account` a
LEFT JOIN latest_per_account l USING (account_id)
LEFT JOIN agg_90d agg USING (account_id)
;
