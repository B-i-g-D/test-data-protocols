# dbt -> Trino -> S3 (Delta-only)

This repository now contains only one pipeline:
- `dbt` transformations,
- `Trino` SQL engine,
- `MinIO` as S3-compatible storage,
- output tables in `Delta` format only.

## Pipeline

Flow:
1. `dbt seed` writes `delta.raw.titanic_seed`
2. `dbt run` builds `delta.analytics.survival_by_class`
3. Delta table files are stored in MinIO bucket `warehouse`
4. data is read back through Trino

## Run

```bash
make pipeline-run
```

## Read

Default read:

```bash
make pipeline-read
```

Custom table + limit:

```bash
./scripts/read_trino_table.sh delta.analytics.survival_by_class 50
```

## Infra

- Compose file: `infra/docker-compose.trino.yml`
- Trino Delta catalog: `infra/trino/etc/catalog/delta.properties`
- dbt project: `dbt/trino_pipeline/`

## Notebook

Main notebook:
- [kaggle_s3_lake_pipeline.ipynb]
- 
## Stop

```bash
make pipeline-down
```
