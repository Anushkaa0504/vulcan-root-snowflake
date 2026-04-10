MODEL (
  name ECOMMERCE_PLATFORM.staging.stg_orders,
  kind VIEW
);

SELECT
    o_orderkey,
    o_custkey,
    o_orderdate,
    o_totalprice,
    1 AS total_events
FROM ECOMMERCE_PLATFORM.raw.raw_orders;