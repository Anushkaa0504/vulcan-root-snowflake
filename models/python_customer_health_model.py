import pandas as pd
from vulcan import ExecutionContext, model, ModelKindName


@model(
    "ECOMMERCE_PLATFORM.vulcan_example.python_customer_health_model",
    kind=dict(name=ModelKindName.FULL),
    depends_on=["ECOMMERCE_PLATFORM.sales.customer_segments"],
    columns={
        "O_CUSTKEY": "int",
        "SPEND_TIER": "text",
        "ORDER_FREQUENCY_SEGMENT": "text",
        "TOTAL_ORDERS": "int",
        "TOTAL_SPENT": "float",
        "REVENUE_PER_ORDER": "float",
        "HEALTH_SCORE": "float",
        "HEALTH_STATUS": "text",
        "MODEL_VERSION": "text",
        "RUN_TS": "timestamp",
    },
)
def execute(context: ExecutionContext, **kwargs):
    segments_table = context.resolve_table("ECOMMERCE_PLATFORM.sales.customer_segments")

    df = context.fetchdf(
        f"""
        SELECT
            O_CUSTKEY,
            SPEND_TIER,
            ORDER_FREQUENCY_SEGMENT,
            TOTAL_ORDERS,
            TOTAL_SPENT,
            REVENUE_PER_ORDER
        FROM {segments_table}
        """
    )

    df.columns = [c.lower() for c in df.columns]
    df["total_orders"] = pd.to_numeric(df["total_orders"], errors="coerce").fillna(0)
    df["total_spent"] = pd.to_numeric(df["total_spent"], errors="coerce").fillna(0.0)
    df["revenue_per_order"] = pd.to_numeric(df["revenue_per_order"], errors="coerce").fillna(0.0)

    spend_score = pd.Series(0.0, index=df.index)
    spend_score += (df["spend_tier"] == "PLATINUM") * 1.0
    spend_score += (df["spend_tier"] == "GOLD") * 0.8
    spend_score += (df["spend_tier"] == "SILVER") * 0.5
    spend_score += (df["spend_tier"] == "BRONZE") * 0.2

    freq_score = pd.Series(0.0, index=df.index)
    freq_score += (df["order_frequency_segment"] == "FREQUENT") * 1.0
    freq_score += (df["order_frequency_segment"] == "ACTIVE") * 0.6
    freq_score += (df["order_frequency_segment"] == "OCCASIONAL") * 0.2

    monetary_score = (df["total_spent"].rank(pct=True, method="average")).fillna(0.0)
    efficiency_score = (df["revenue_per_order"].rank(pct=True, method="average")).fillna(0.0)

    df["health_score"] = (
        0.35 * spend_score
        + 0.30 * freq_score
        + 0.20 * monetary_score
        + 0.15 * efficiency_score
    ).round(4)

    df["health_status"] = pd.cut(
        df["health_score"],
        bins=[-0.001, 0.35, 0.70, 1.0],
        labels=["AT_RISK", "STABLE", "THRIVING"],
    ).astype("string")

    df["model_version"] = "customer_health_v1"
    df["run_ts"] = pd.Timestamp.utcnow()

    return df.rename(
        columns={
            "o_custkey": "O_CUSTKEY",
            "spend_tier": "SPEND_TIER",
            "order_frequency_segment": "ORDER_FREQUENCY_SEGMENT",
            "total_orders": "TOTAL_ORDERS",
            "total_spent": "TOTAL_SPENT",
            "revenue_per_order": "REVENUE_PER_ORDER",
            "health_score": "HEALTH_SCORE",
            "health_status": "HEALTH_STATUS",
            "model_version": "MODEL_VERSION",
            "run_ts": "RUN_TS",
        }
    )[
        [
            "O_CUSTKEY",
            "SPEND_TIER",
            "ORDER_FREQUENCY_SEGMENT",
            "TOTAL_ORDERS",
            "TOTAL_SPENT",
            "REVENUE_PER_ORDER",
            "HEALTH_SCORE",
            "HEALTH_STATUS",
            "MODEL_VERSION",
            "RUN_TS",
        ]
    ]
