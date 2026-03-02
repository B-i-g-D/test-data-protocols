#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

.venv/bin/pip install -U pip >/dev/null
.venv/bin/pip install -e . >/dev/null

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
  cp .env.example .env
fi

if [ -z "${SSL_CERT_FILE:-}" ]; then
  SSL_CERT_FILE="$(${ROOT_DIR}/.venv/bin/python -c 'import certifi; print(certifi.where())')"
  export SSL_CERT_FILE
fi

PYTHONPATH=src .venv/bin/kaggle-s3-lake ingest

echo

echo "Run completed. Artifacts:"
echo "- ./data_lake/kaggle-datasets/titanic/parquet/data.parquet"
echo "- ./data_lake/kaggle-datasets/titanic/delta"
echo "- Iceberg table: local.kaggle.titanic"
echo "- ./data_lake/kaggle-datasets/titanic/manifest.json"
echo "- ./data_lake/kaggle-datasets/titanic/metrics.json"
