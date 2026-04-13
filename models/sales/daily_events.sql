MODEL (
  name ECOMMERCE_PLATFORM.sales.daily_events,
  kind FULL,
  grains (event_date),
  description 'Daily event aggregation'
);


SELECT
  o_orderdate AS event_date,
  COUNT(o_orderkey) AS total_events,
  COUNT(DISTINCT o_custkey) AS unique_items
FROM ECOMMERCE_PLATFORM.staging.stg_orders
GROUP BY o_orderdate
ORDER BY o_orderdate
