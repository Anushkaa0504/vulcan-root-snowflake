import pandas as pd
from datetime import datetime
from vulcan import ExecutionContext, ModelKindName, model


@model(
    "ECOMMERCE_PLATFORM.vulcan_example.python_spcs_job_model",
    kind=dict(name=ModelKindName.FULL),
    depends_on=["ECOMMERCE_PLATFORM.mart.features"],
    columns={
        "JOB_NAME": "text",
        "STATUS": "text",
        "MESSAGE": "text",
    },
)
def execute(context: ExecutionContext, execution_time: datetime, **kwargs):
    context.resolve_table("ECOMMERCE_PLATFORM.mart.features")
    return pd.DataFrame(
        [
            {
                "JOB_NAME": "",
                "STATUS": "SKIPPED",
                "MESSAGE": "SPCS job temporarily disabled.",
            }
        ]
    )
