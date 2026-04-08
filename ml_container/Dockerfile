FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libffi-dev libssl-dev wget curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install --no-cache-dir \
    "snowflake-snowpark-python[pandas]" \
    "snowflake-connector-python[pandas]" \
    scikit-learn

COPY ml_entrypoint.py .

CMD ["python", "ml_entrypoint.py"]
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libffi-dev libssl-dev wget curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install --no-cache-dir \
    "snowflake-snowpark-python[pandas]" \
    "snowflake-connector-python[pandas]" \
    scikit-learn

COPY ml_entrypoint.py .

CMD ["python", "ml_entrypoint.py"]
