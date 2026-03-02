#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE_FILE="infra/docker-compose.trino.yml"
DBT_DIR="dbt/trino_pipeline"

if [[ ! -f "${DBT_DIR}/profiles.yml" ]]; then
  cp "${DBT_DIR}/profiles.yml.example" "${DBT_DIR}/profiles.yml"
fi
if grep -q "host: localhost" "${DBT_DIR}/profiles.yml"; then
  sed -i.bak 's/host: localhost/host: trino/g' "${DBT_DIR}/profiles.yml"
  rm -f "${DBT_DIR}/profiles.yml.bak"
fi

docker compose -f "${COMPOSE_FILE}" up -d --remove-orphans

until docker compose -f "${COMPOSE_FILE}" exec -T trino /usr/lib/trino/bin/health-check >/dev/null 2>&1; do
  echo "Waiting for Trino..."
  sleep 3
done

if [[ -x ".venv/bin/python" ]]; then
  .venv/bin/python - <<'PY'
import trino

conn = trino.dbapi.connect(host="localhost", port=8080, user="dbt", catalog="delta", schema="default")
cur = conn.cursor()
cur.execute("drop schema if exists raw cascade")
cur.fetchall()
cur.execute("drop schema if exists analytics cascade")
cur.fetchall()
cur.execute("create schema if not exists raw")
cur.fetchall()
cur.execute("create schema if not exists analytics")
cur.fetchall()
PY
fi

docker compose -f "${COMPOSE_FILE}" exec -T dbt dbt debug --project-dir /workspace/${DBT_DIR} --profiles-dir /workspace/${DBT_DIR}
docker compose -f "${COMPOSE_FILE}" exec -T dbt dbt seed --full-refresh --project-dir /workspace/${DBT_DIR} --profiles-dir /workspace/${DBT_DIR}
docker compose -f "${COMPOSE_FILE}" exec -T dbt dbt run --project-dir /workspace/${DBT_DIR} --profiles-dir /workspace/${DBT_DIR}

echo "Pipeline finished: dbt -> Trino -> S3 (Delta-only)."
