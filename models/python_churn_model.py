import pandas as pd
from vulcan import ExecutionContext, model, ModelKindName


@model(
    "ECOMMERCE_PLATFORM.vulcan_example.python_churn_model",
    kind=dict(name=ModelKindName.FULL),
    depends_on=["ECOMMERCE_PLATFORM.mart.features"],
    columns={
        "O_CUSTKEY": "int",
        "TOTAL_ORDERS": "int",
        "AVG_ORDER_VALUE": "float",
        "TOTAL_SPENT": "float",
        "CHURN_SCORE": "float",
        "CHURN_SEGMENT": "text",
        "RUN_TS": "timestamp",
        "MODEL_VERSION": "text",
    },
)
def execute(context: ExecutionContext, **kwargs):
    features_table = context.resolve_table("ECOMMERCE_PLATFORM.mart.features")

    df = context.fetchdf(f"""
        SELECT
            O_CUSTKEY,
            TOTAL_ORDERS,
            AVG_ORDER_VALUE,
            TOTAL_SPENT
        FROM {features_table}
    """)

    df["TOTAL_ORDERS"] = pd.to_numeric(df["TOTAL_ORDERS"], errors="coerce").fillna(0.0)
    df["AVG_ORDER_VALUE"] = pd.to_numeric(df["AVG_ORDER_VALUE"], errors="coerce").fillna(0.0)
    df["TOTAL_SPENT"] = pd.to_numeric(df["TOTAL_SPENT"], errors="coerce").fillna(0.0)

    # Risk is higher for customers with lower order frequency/spend/value.
    freq_risk = 1.0 - df["TOTAL_ORDERS"].rank(pct=True, method="average")
    spend_risk = 1.0 - df["TOTAL_SPENT"].rank(pct=True, method="average")
    aov_risk = 1.0 - df["AVG_ORDER_VALUE"].rank(pct=True, method="average")

    df["CHURN_SCORE"] = (0.5 * freq_risk + 0.35 * spend_risk + 0.15 * aov_risk).clip(0.0, 1.0).round(4)
    df["CHURN_SEGMENT"] = pd.cut(
        df["CHURN_SCORE"],
        bins=[-0.001, 0.35, 0.70, 1.0],
        labels=["LOW", "MEDIUM", "HIGH"],
    ).astype("string")

    df["RUN_TS"] = pd.Timestamp.utcnow()
    df["MODEL_VERSION"] = "churn_v1"


    return df[
        [
            "O_CUSTKEY",
            "TOTAL_ORDERS",
            "AVG_ORDER_VALUE",
            "TOTAL_SPENT",
            "CHURN_SCORE",
            "CHURN_SEGMENT",
            "RUN_TS",
            "MODEL_VERSION",
        ]
    ]
