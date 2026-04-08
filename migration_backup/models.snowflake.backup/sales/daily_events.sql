MODEL (
  name sales.daily_events,
  kind FULL,
  cron '@daily',
  grains (event_date),
  description 'Daily event aggregation'
);

SELECT
  o_orderdate AS event_date,
  COUNT(o_orderkey) AS total_events,
  COUNT(DISTINCT o_custkey) AS unique_items
FROM DEMO_DB.RAW.RAW_ORDERS
GROUP BY o_orderdate
ORDER BY o_orderdate