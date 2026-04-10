import os
from datetime import datetime

import pandas as pd
from vulcan import ExecutionContext, ModelKindName, model


def _esc(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"')


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

    # Required to actually run SPCS job.
    # Example:
    # export SPCS_IMAGE="/DEMO_DB/VULCAN_EXAMPLE/ML_REPO/my_spcs_ml:latest"
    image = os.getenv("SPCS_IMAGE", "").strip()
    if not image:
        return pd.DataFrame(
            [
                {
                    "JOB_NAME": "",
                    "STATUS": "SKIPPED",
                    "MESSAGE": "SPCS_IMAGE is not set; skipping EXECUTE JOB SERVICE.",
                }
            ]
        )

    job_name = f"ML_JOB_{execution_time.strftime('%Y%m%d_%H%M%S')}".upper()
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    job_db = os.getenv("SPCS_JOB_DB", "ECOMMERCE_PLATFORM")
    job_schema = os.getenv("SPCS_JOB_SCHEMA", "VULCAN_EXAMPLE")

    sf_user = _esc(os.getenv("SNOWFLAKE_USER", ""))
    sf_pass = _esc(os.getenv("SNOWFLAKE_PASSWORD", ""))
    sf_acct = _esc(os.getenv("SNOWFLAKE_ACCOUNT", ""))
    sf_db = _esc(os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_PLATFORM"))
    sf_schema = _esc(os.getenv("SNOWFLAKE_SCHEMA", "VULCAN_EXAMPLE"))

    context.fetchdf(
        """
        CREATE COMPUTE POOL IF NOT EXISTS ML_POOL_LAB
        MIN_NODES = 1
        MAX_NODES = 1
        INSTANCE_FAMILY = CPU_X64_XS
        AUTO_RESUME = TRUE
        AUTO_SUSPEND_SECS = 120
        """
    )

    context.fetchdf(
        f"""
        EXECUTE JOB SERVICE
          IN COMPUTE POOL ML_POOL_LAB
          NAME = {job_db}.{job_schema}.{job_name}
          ASYNC = TRUE
          QUERY_WAREHOUSE = {warehouse}
          FROM SPECIFICATION $$
spec:
  containers:
    - name: ml
      image: {image}
      env:
        SNOWFLAKE_USER: "{sf_user}"
        SNOWFLAKE_PASSWORD: "{sf_pass}"
        SNOWFLAKE_ACCOUNT: "{sf_acct}"
        SNOWFLAKE_WAREHOUSE: "{warehouse}"
        SNOWFLAKE_DATABASE: "{sf_db}"
        SNOWFLAKE_SCHEMA: "{sf_schema}"
$$
        """
    )

    return pd.DataFrame(
        [{"JOB_NAME": job_name, "STATUS": "SUBMITTED", "MESSAGE": f"Image: {image}"}]
    )
