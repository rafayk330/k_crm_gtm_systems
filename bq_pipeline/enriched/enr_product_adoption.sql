WITH
  feature_usage_30d AS (
  SELECT
    product_id,
    account_id,
    COUNT(*) AS uses_30d,
    COUNT(DISTINCT account_id) AS unique_users_30d -- Need TO use unique 'contact_id' here, but the dataset doesn't have any, so' account_id' IS being used
  FROM
    `k_crm_core.core_fct_feature_adoption`
  WHERE
    used_at >= TIMESTAMP_SUB("2024-01-01", INTERVAL 30 DAY)
  GROUP BY
    product_id,
    account_id ),
  product_summary AS (
  SELECT
    f.product_id,
    COUNT(DISTINCT f.account_id) AS active_accounts_30d,
    SUM(f.uses_30d) AS total_uses_30d,
    AVG(f.uses_30d) AS avg_uses_per_account
  FROM
    feature_usage_30d f
  GROUP BY
    f.product_id ),
  total_accounts AS (
  SELECT
    COUNT(DISTINCT account_id) AS total_accounts_a
  FROM
    `k_crm_core.core_dim_account` )
SELECT
  p.product_id,
  COALESCE(ps.active_accounts_30d,0) AS active_accounts_30d,
  COALESCE(ps.total_uses_30d,0) AS total_uses_30d,
  COALESCE(ps.avg_uses_per_account,0) AS avg_uses_per_account,
  -- adoption_score: % OF accounts USING product * scaled avg usage
  ROUND( SAFE_DIVIDE(COALESCE(ps.active_accounts_30d,0), (
      SELECT
        total_accounts_a
      FROM
        total_accounts)) * 100 * 0.6 + LEAST(100, COALESCE(ps.avg_uses_per_account,0) * 10) * 0.4,2) AS adoption_score,
  "2024-01-01" AS updated_at
FROM
  `k_crm_core.core_dim_product` p
LEFT JOIN
  product_summary ps
USING
  (product_id) ;
