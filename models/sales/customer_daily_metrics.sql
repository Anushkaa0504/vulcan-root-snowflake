MODEL (
  name ECOMMERCE_PLATFORM.sales.customer_daily_metrics,
  kind FULL,
  grains (order_date, o_custkey),
  description 'Daily order and revenue metrics by customer'
);

SELECT
  o_orderdate AS order_date,
  o_custkey,
  COUNT(*) AS total_orders,
  SUM(o_totalprice) AS total_revenue,
  AVG(o_totalprice) AS avg_order_value
FROM ECOMMERCE_PLATFORM.staging.stg_orders
GROUP BY o_orderdate, o_custkey
ORDER BY order_date, o_custkey
