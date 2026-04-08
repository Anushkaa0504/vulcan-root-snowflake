MODEL (
  name sales.customer_monthly_metrics,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column month_start,
    batch_size 30
  ),
  start '2025-01-01',
  cron '@monthly',
  grains (month_start, o_custkey),
  description 'Monthly order and revenue metrics by customer'
);

SELECT
  DATE_TRUNC('month', o_orderdate) AS month_start,
  o_custkey,
  COUNT(*) AS total_orders,
  SUM(o_totalprice) AS total_revenue,
  AVG(o_totalprice) AS avg_order_value
FROM staging.stg_orders
WHERE o_orderdate BETWEEN @start_ds AND @end_ds
GROUP BY DATE_TRUNC('month', o_orderdate), o_custkey
ORDER BY month_start, o_custkey
