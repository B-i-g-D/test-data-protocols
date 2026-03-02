FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core==1.10.13 dbt-trino==1.10.1

WORKDIR /workspace/dbt/trino_pipeline
CMD ["sleep", "infinity"]
