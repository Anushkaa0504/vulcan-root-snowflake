MODEL (
  name ECOMMERCE_PLATFORM.sales.customer_segments,
  kind FULL,
  grains (o_custkey),
  description 'Customer spend and order frequency segments derived from mart.features'
);

SELECT
  o_custkey,
  total_orders,
  avg_order_value,
  total_spent,
  ROUND(total_spent / NULLIF(total_orders, 0), 2) AS revenue_per_order,
  CASE
    WHEN total_spent >= 100000 THEN 'PLATINUM'
    WHEN total_spent >= 50000 THEN 'GOLD'
    WHEN total_spent >= 10000 THEN 'SILVER'
    ELSE 'BRONZE'
  END AS spend_tier,
  CASE
    WHEN total_orders >= 50 THEN 'FREQUENT'
    WHEN total_orders >= 10 THEN 'ACTIVE'
    ELSE 'OCCASIONAL'
  END AS order_frequency_segment
FROM mart.features
