import os
from datetime import datetime
import pandas as pd
from vulcan import ExecutionContext, model, ModelKindName


def _esc(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"')


@model(
    "vulcan_example.python_spcs_job_model",
    kind=dict(name=ModelKindName.FULL),
    columns={
        "JOB_NAME": "text",
        "STATUS": "text",
    },
)
def execute(context: ExecutionContext, execution_time: datetime, **kwargs):
    context.resolve_table("mart.features")

    job_name = f"ML_JOB_{execution_time.strftime('%Y%m%d_%H%M%S')}".upper()
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    sf_user = _esc(os.getenv("SNOWFLAKE_USER", ""))
    sf_pass = _esc(os.getenv("SNOWFLAKE_PASSWORD", ""))
    sf_acct = _esc(os.getenv("SNOWFLAKE_ACCOUNT", ""))
    sf_db = _esc(os.getenv("SNOWFLAKE_DATABASE", "DEMO_DB"))
    sf_schema = _esc(os.getenv("SNOWFLAKE_SCHEMA", "VULCAN_EXAMPLE"))

    context.fetchdf("""
        CREATE COMPUTE POOL IF NOT EXISTS ML_POOL_LAB
        MIN_NODES = 1
        MAX_NODES = 1
        INSTANCE_FAMILY = CPU_X64_XS
        AUTO_RESUME = TRUE
        AUTO_SUSPEND_SECS = 120
    """)

    context.fetchdf(
        f"""
        EXECUTE JOB SERVICE
          IN COMPUTE POOL ML_POOL_LAB
          NAME = DEMO_DB.VULCAN_EXAMPLE.{job_name}
          ASYNC = TRUE
          QUERY_WAREHOUSE = {warehouse}
          FROM SPECIFICATION $$
spec:
  containers:
    - name: ml
      image: /DEMO_DB/VULCAN_EXAMPLE/ML_REPO/my_spcs_ml:latest
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

    return pd.DataFrame([{"JOB_NAME": job_name, "STATUS": "SUBMITTED"}])
