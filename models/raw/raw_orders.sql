MODEL (
  name raw.raw_orders,
  kind VIEW
);

SELECT
    o_orderkey,
    o_custkey,
    o_orderdate,
    o_totalprice
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;