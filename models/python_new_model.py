import pandas as pd
from vulcan import ExecutionContext, model, ModelKindName

@model(
    "ECOMMERCE_PLATFORM.vulcan_example.python_new_model",
    kind=dict(name=ModelKindName.FULL),
    columns={
        "O_CUSTKEY": "int",
        "TOTAL_ORDERS": "int",
        "AVG_ORDER_VALUE": "float",
        "TOTAL_SPENT": "float",
        "PREDICTED_SPEND": "float",
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

    # 👇 IMPORTANT: DO NOT lowercase
    df["PREDICTED_SPEND"] = df["TOTAL_ORDERS"] * df["AVG_ORDER_VALUE"]

    return df