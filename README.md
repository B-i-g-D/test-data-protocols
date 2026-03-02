# Public Dataset -> Local Multi-Format Lake

Проєкт завантажує публічний датасет і зберігає його локально одразу в 3 форматах:
- Parquet
- Delta Lake
- Iceberg

Також є read-функціонал для кожного формату.

## Швидкий старт

```bash
make run
```

Що зробить `make run`:
- створить `.venv` (якщо нема),
- встановить залежності,
- підхопить `.env.example` (якщо `.env` відсутній),
- виконає `ingest` з дефолтним публічним Titanic URL.

Альтернатива для PyCharm/IDE:
- просто натисни `Run` для [main.py](/Users/dmytrosylenok/PycharmProjects/test-data-protocols/main.py) (без аргументів).

## 1) Встановлення вручну

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## 2) Конфіг

```bash
cp .env.example .env
```

Заповни `.env`:
- `LAKE_ROOT` (локальна папка lake, за замовчуванням `./data_lake`)
- `ICEBERG_CATALOG_URI` (де тримати метадані Iceberg каталогу)

## 3) Імпорт датасету в локальний lake

Найпростіший запуск (повністю на дефолтах):
```bash
kaggle-s3-lake ingest
```

Це використає:
- source URL: `https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv`
- dataset id: `titanic`
- lake root: `./data_lake` (або `LAKE_ROOT` з `.env`)
- prefix: `kaggle-datasets`

Кастомний запуск:
```bash
kaggle-s3-lake ingest \
  --source-url https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv \
  --dataset-id titanic \
  --prefix kaggle-datasets \
  --namespace kaggle \
  --catalog-name local
```

Опціонально:
- `--lake-root ./data_lake`
- `--sample-limit 1000` щоб обмежити обсяг

Після ingest буде створено:
- `<lake_root>/<prefix>/<dataset_id>/parquet/data.parquet`
- `<lake_root>/<prefix>/<dataset_id>/delta/`
- Iceberg table в namespace/table з warehouse `<lake_root>/<prefix>/iceberg`
- `<lake_root>/<prefix>/<dataset_id>/manifest.json`
- `<lake_root>/<prefix>/<dataset_id>/metrics.json`

## 4) Читання назад

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

## Логування і метрики

- Логи виводяться автоматично на рівні `INFO`.
- Після `ingest` формується `metrics.json` з таймінгами етапів і розмірами артефактів.
- Під час `read` у stdout виводиться `read_seconds`.

## Тести

```bash
pip install -e ".[dev]"
PYTHONPATH=src pytest -q
```

## Make targets

```bash
make setup
make run
make test
```
