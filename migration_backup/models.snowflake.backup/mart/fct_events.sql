MODEL (
  name mart.fct_events,
  kind FULL
);

SELECT
    o_custkey,
    COUNT(*) AS total_orders,
    AVG(o_totalprice) AS avg_order_value,
    SUM(o_totalprice) AS total_spent
FROM staging.stg_orders
GROUP BY o_custkey;