MODEL (
  name sales.customer_daily_metrics,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_date,
    batch_size 1
  ),
  start '2025-01-01',
  cron '@daily',
  signals (
    stabilized_intervals(days := 1)
  ),
  grains (order_date, o_custkey),
  description 'Daily order and revenue metrics by customer'
);

SELECT
  o_orderdate AS order_date,
  o_custkey,
  COUNT(*) AS total_orders,
  SUM(o_totalprice) AS total_revenue,
  AVG(o_totalprice) AS avg_order_value
FROM staging.stg_orders
WHERE o_orderdate BETWEEN @start_ds AND @end_ds
GROUP BY o_orderdate, o_custkey
ORDER BY order_date, o_custkey
