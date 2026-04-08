MODEL (
  name sales.daily_events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date,
    batch_size 1
  ),
  start '2025-01-01',
  cron '@daily',
  grains (event_date),
  description 'Daily event aggregation'
);

SELECT
  o_orderdate AS event_date,
  COUNT(o_orderkey) AS total_events,
  COUNT(DISTINCT o_custkey) AS unique_items
FROM DEMO_DB.RAW.RAW_ORDERS
WHERE o_orderdate BETWEEN @start_ds AND @end_ds
GROUP BY o_orderdate
ORDER BY o_orderdate
