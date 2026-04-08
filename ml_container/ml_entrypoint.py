import os
from snowflake.snowpark import Session
from sklearn.linear_model import LinearRegression

conn_params = {
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
}

session = Session.builder.configs(conn_params).create()

try:
    print("Connected to Snowflake!")

    # READ FEATURES CREATED BY VULCAN
    df = session.table("FEATURES").to_pandas()
    df.columns = [c.lower() for c in df.columns]

    X = df[["total_orders", "avg_order_value"]]
    y = df["total_spent"]

    model = LinearRegression()
    model.fit(X, y)

    df["predicted_spend"] = model.predict(X)

    #  WRITE BACK TO SNOWFLAKE
    session.write_pandas(
        df[["o_custkey", "predicted_spend"]],
        "ML_PREDICTIONS",
        auto_create_table=True,
        overwrite=True
    )

    print("Predictions written!")

finally:
    session.close()