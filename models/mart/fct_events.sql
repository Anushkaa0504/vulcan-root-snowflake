MODEL (
  name ECOMMERCE_PLATFORM.mart.fct_events,
  kind FULL,
  columns (
    EVENT_DATE DATE,
    ITEM_ID BIGINT,
    TOTAL_EVENTS BIGINT
  ),
  grains (EVENT_DATE, ITEM_ID),
  description 'Event aggregates by date and item for semantic layer'
);

SELECT
  o.o_orderdate AS EVENT_DATE,
  l.l_partkey AS ITEM_ID,
  COUNT(*) AS TOTAL_EVENTS
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM l
  ON o.o_orderkey = l.l_orderkey
GROUP BY 1, 2
