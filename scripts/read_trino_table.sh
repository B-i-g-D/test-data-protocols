#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE_FILE="infra/docker-compose.trino.yml"
TABLE_NAME="${1:-delta.analytics.survival_by_class}"
LIMIT="${2:-20}"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Python venv not found at .venv/bin/python"
  exit 1
fi

.venv/bin/python - <<PY
import trino

table_name = "${TABLE_NAME}"
limit = int("${LIMIT}")

conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="dbt",
    catalog="delta",
    schema="analytics",
)
cur = conn.cursor()
cur.execute(f"select * from {table_name} limit {limit}")
rows = cur.fetchall()
cols = [c[0] for c in cur.description]
print(" | ".join(cols))
for r in rows:
    print(" | ".join(str(x) for x in r))
PY
