MODEL (
  name mart.features,
  kind FULL
);

SELECT
    o_custkey,
    total_orders,
    avg_order_value,
    total_spent
FROM mart.fct_events;