import pandas as pd
from vulcan import ExecutionContext, model, ModelKindName


@model(
    "ECOMMERCE_PLATFORM.vulcan_example.python_customer_segment_summary",
    kind=dict(name=ModelKindName.FULL),
    depends_on=["ECOMMERCE_PLATFORM.sales.customer_segments"],
    columns={
        "SPEND_TIER": "text",
        "ORDER_FREQUENCY_SEGMENT": "text",
        "CUSTOMER_COUNT": "int",
        "TOTAL_ORDERS": "int",
        "TOTAL_SPENT": "float",
        "AVG_REVENUE_PER_ORDER": "float",
        "SEGMENT_SHARE": "float",
        "MODEL_VERSION": "text",
        "RUN_TS": "timestamp",
    },
)
def execute(context: ExecutionContext, **kwargs):
    segments_table = context.resolve_table("ECOMMERCE_PLATFORM.sales.customer_segments")

    df = context.fetchdf(
        f"""
        SELECT
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

    out = (
        df.groupby(["spend_tier", "order_frequency_segment"], dropna=False)
        .agg(
            customer_count=("spend_tier", "size"),
            total_orders=("total_orders", "sum"),
            total_spent=("total_spent", "sum"),
            avg_revenue_per_order=("revenue_per_order", "mean"),
        )
        .reset_index()
    )

    total_customers = max(int(out["customer_count"].sum()), 1)
    out["segment_share"] = (out["customer_count"] / total_customers).round(4)
    out["avg_revenue_per_order"] = out["avg_revenue_per_order"].round(4)
    out["model_version"] = "customer_segment_summary_v1"
    out["run_ts"] = pd.Timestamp.utcnow()

    return out.rename(
        columns={
            "spend_tier": "SPEND_TIER",
            "order_frequency_segment": "ORDER_FREQUENCY_SEGMENT",
            "customer_count": "CUSTOMER_COUNT",
            "total_orders": "TOTAL_ORDERS",
            "total_spent": "TOTAL_SPENT",
            "avg_revenue_per_order": "AVG_REVENUE_PER_ORDER",
            "segment_share": "SEGMENT_SHARE",
            "model_version": "MODEL_VERSION",
            "run_ts": "RUN_TS",
        }
    )[
        [
            "SPEND_TIER",
            "ORDER_FREQUENCY_SEGMENT",
            "CUSTOMER_COUNT",
            "TOTAL_ORDERS",
            "TOTAL_SPENT",
            "AVG_REVENUE_PER_ORDER",
            "SEGMENT_SHARE",
            "MODEL_VERSION",
            "RUN_TS",
        ]
    ]
