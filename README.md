# Public Dataset -> Local Multi-Format Lake

This project downloads a public dataset and stores it locally in three formats:
- Parquet
- Delta Lake
- Iceberg

It also provides read functionality for each format.

## Quick Start

```bash
make run
```

What `make run` does:
- creates `.venv` if it does not exist,
- installs dependencies,
- copies `.env.example` to `.env` if needed,
- runs `ingest` with the default public Titanic URL.

Alternative for PyCharm/IDE:
- click `Run` on [main.py](/Users/dmytrosylenok/PycharmProjects/test-data-protocols/main.py) with no arguments.

## 1) Manual Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## 2) Config

```bash
cp .env.example .env
```

Fill `.env`:
- `LAKE_ROOT` (local lake folder, default `./data_lake`)
- `ICEBERG_CATALOG_URI` (where Iceberg catalog metadata is stored)

## 3) Ingest Dataset to Local Lake

Simplest run (all defaults):
```bash
kaggle-s3-lake ingest
```

Defaults:
- source URL: `https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv`
- dataset id: `titanic`
- lake root: `./data_lake` (or `LAKE_ROOT` from `.env`)
- prefix: `kaggle-datasets`

Custom run:
```bash
kaggle-s3-lake ingest \
  --source-url https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv \
  --dataset-id titanic \
  --prefix kaggle-datasets \
  --namespace kaggle \
  --catalog-name local
```

Optional:
- `--lake-root ./data_lake`
- `--sample-limit 1000` to limit data size

After ingest, the project creates:
- `<lake_root>/<prefix>/<dataset_id>/parquet/data.parquet`
- `<lake_root>/<prefix>/<dataset_id>/delta/`
- Iceberg table in namespace/table with warehouse `<lake_root>/<prefix>/iceberg`
- `<lake_root>/<prefix>/<dataset_id>/manifest.json`
- `<lake_root>/<prefix>/<dataset_id>/metrics.json`

## 4) Read Back

### Parquet
```bash
kaggle-s3-lake read --format parquet --uri ./data_lake/kaggle-datasets/titanic/parquet/data.parquet
```

### Delta
```bash
kaggle-s3-lake read --format delta --uri ./data_lake/kaggle-datasets/titanic/delta
```

### Iceberg
```bash
kaggle-s3-lake read \
  --format iceberg \
  --catalog-name local \
  --namespace kaggle \
  --table-name titanic \
  --lake-root ./data_lake \
  --prefix kaggle-datasets
```

## Logging and Metrics

- Logs are emitted at `INFO` level by default.
- `ingest` generates `metrics.json` with step timings and artifact sizes.
- `read` prints `read_seconds` to stdout.

## Tests

```bash
pip install -e ".[dev]"
PYTHONPATH=src pytest -q
```

## Make Targets

```bash
make setup
make run
make test
```
