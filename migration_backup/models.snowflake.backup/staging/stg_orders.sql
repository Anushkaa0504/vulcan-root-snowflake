MODEL (
  name staging.stg_orders,
  kind VIEW
);

SELECT
    o_orderkey,
    o_custkey,
    o_orderdate,
    o_totalprice,
    1 AS total_events
FROM raw.raw_orders;