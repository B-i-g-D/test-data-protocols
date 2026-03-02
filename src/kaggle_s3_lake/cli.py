from __future__ import annotations

import argparse
import json
import logging
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .kaggle_download import download_file_from_url, load_dataframe
from .readers import read_delta, read_iceberg, read_parquet
from .writers import dataframe_schema, write_delta, write_iceberg, write_parquet

LOGGER = logging.getLogger("kaggle_s3_lake")
DEFAULT_LAKE_ROOT = "./data_lake"
DEFAULT_SOURCE_URL = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
DEFAULT_DATASET_ID = "titanic"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Public dataset -> Local lake (Parquet, Delta, Iceberg) with readback")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="Download a public dataset and save all formats locally")
    ingest.add_argument(
        "--source-url",
        default=DEFAULT_SOURCE_URL,
        help=f"Public file URL (CSV/TSV/JSON/Parquet). Default: {DEFAULT_SOURCE_URL}",
    )
    ingest.add_argument(
        "--dataset-id",
        default=DEFAULT_DATASET_ID,
        help=f"Logical dataset id used in local paths and Iceberg table name. Default: {DEFAULT_DATASET_ID}",
    )
    ingest.add_argument(
        "--lake-root",
        default=None,
        help=f"Local lake root. Default from LAKE_ROOT or {DEFAULT_LAKE_ROOT}",
    )
    ingest.add_argument("--prefix", default="kaggle-datasets", help="Local prefix inside lake root")
    ingest.add_argument("--namespace", default="kaggle", help="Iceberg namespace")
    ingest.add_argument("--table-name", help="Iceberg table name. Defaults to dataset-id with underscores")
    ingest.add_argument("--catalog-name", default="local", help="PyIceberg catalog logical name")
    ingest.add_argument("--sample-limit", type=int, help="Optional number of rows to keep")

    read_cmd = subparsers.add_parser("read", help="Read data back from one format")
    read_cmd.add_argument("--format", required=True, choices=["parquet", "delta", "iceberg"])
    read_cmd.add_argument("--uri", help="Local path for parquet or delta")
    read_cmd.add_argument("--limit", type=int, default=10, help="Rows to print")
    read_cmd.add_argument("--catalog-name", default="local")
    read_cmd.add_argument("--namespace", default="kaggle")
    read_cmd.add_argument("--table-name")
    read_cmd.add_argument(
        "--lake-root",
        default=None,
        help=f"Local lake root. Default from LAKE_ROOT or {DEFAULT_LAKE_ROOT}",
    )
    read_cmd.add_argument("--prefix", default="kaggle-datasets")

    return parser


def _table_name_from_dataset(dataset: str) -> str:
    return dataset.replace("/", "_").replace("-", "_").lower()


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def run_ingest(args: argparse.Namespace) -> None:
    settings = Settings.from_env()
    lake_root = Path(args.lake_root or settings.lake_root or DEFAULT_LAKE_ROOT).resolve()
    metrics: dict[str, object] = {
        "event": "ingest",
        "dataset": args.dataset_id,
        "source_url": args.source_url,
        "started_at": _now_iso(),
        "steps": {},
    }

    LOGGER.info("Starting ingest for dataset-id '%s'", args.dataset_id)
    LOGGER.debug("Lake root resolved to: %s", lake_root)

    t0 = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="kaggle_download_") as tmp:
        downloaded_file = download_file_from_url(args.source_url, Path(tmp))
        df, selected_file = load_dataframe([downloaded_file], preferred_file=None)
    metrics["steps"]["download_and_load_seconds"] = round(time.perf_counter() - t0, 4)
    LOGGER.info("Loaded source file: %s (rows=%s cols=%s)", selected_file, len(df), len(df.columns))

    if args.sample_limit:
        df = df.head(args.sample_limit).copy()
        LOGGER.info("Applied sample limit: %s rows", len(df))

    dataset_key = args.dataset_id.replace("/", "__")
    dataset_root = lake_root / args.prefix / dataset_key
    parquet_uri = str((dataset_root / "parquet" / "data.parquet").resolve())
    delta_uri = str((dataset_root / "delta").resolve())
    _ensure_dir(Path(parquet_uri).parent)
    _ensure_dir(Path(delta_uri))

    t0 = time.perf_counter()
    write_parquet(df, parquet_uri)
    metrics["steps"]["write_parquet_seconds"] = round(time.perf_counter() - t0, 4)
    LOGGER.info("Parquet written: %s", parquet_uri)

    t0 = time.perf_counter()
    write_delta(df, delta_uri)
    metrics["steps"]["write_delta_seconds"] = round(time.perf_counter() - t0, 4)
    LOGGER.info("Delta written: %s", delta_uri)

    table_name = args.table_name or _table_name_from_dataset(args.dataset_id)
    warehouse_uri = str((lake_root / args.prefix / "iceberg").resolve())
    _ensure_dir(Path(warehouse_uri))
    t0 = time.perf_counter()
    iceberg_table_ref = write_iceberg(
        df,
        catalog_name=args.catalog_name,
        namespace=args.namespace,
        table_name=table_name,
        catalog_uri=settings.iceberg_catalog_uri,
        warehouse_uri=warehouse_uri,
    )
    metrics["steps"]["write_iceberg_seconds"] = round(time.perf_counter() - t0, 4)
    LOGGER.info("Iceberg written: %s", iceberg_table_ref)

    manifest_path = (dataset_root / "manifest.json").resolve()
    metrics_path = (dataset_root / "metrics.json").resolve()
    _ensure_dir(manifest_path.parent)
    manifest = {
        "dataset": args.dataset_id,
        "source_url": args.source_url,
        "source_file": str(selected_file),
        "rows": int(len(df)),
        "schema": list(dataframe_schema(df)),
        "artifacts": {
            "parquet": parquet_uri,
            "delta": delta_uri,
            "iceberg": {
                "catalog": args.catalog_name,
                "namespace": args.namespace,
                "table": table_name,
                "ref": iceberg_table_ref,
                "catalog_uri": settings.iceberg_catalog_uri,
                "warehouse_uri": warehouse_uri,
            },
        },
        "read_hints": {
            "python_parquet": f"kaggle-s3-lake read --format parquet --uri {parquet_uri}",
            "python_delta": f"kaggle-s3-lake read --format delta --uri {delta_uri}",
            "python_iceberg": (
                "kaggle-s3-lake read --format iceberg "
                f"--catalog-name {args.catalog_name} --namespace {args.namespace} --table-name {table_name} "
                f"--lake-root {lake_root} --prefix {args.prefix}"
            ),
        },
    }

    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    metrics["finished_at"] = _now_iso()
    metrics["rows"] = int(len(df))
    metrics["columns"] = int(len(df.columns))
    metrics["artifacts"] = {
        "parquet_bytes": _dir_size_bytes(Path(parquet_uri)),
        "delta_bytes": _dir_size_bytes(Path(delta_uri)),
        "iceberg_warehouse_bytes": _dir_size_bytes(Path(warehouse_uri)),
    }
    metrics["steps"]["total_seconds"] = round(
        sum(float(v) for v in metrics["steps"].values() if isinstance(v, (int, float))), 4
    )
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("Ingest completed.")
    print(f"Selected file: {selected_file}")
    print(f"Parquet: {parquet_uri}")
    print(f"Delta:   {delta_uri}")
    print(f"Iceberg: {iceberg_table_ref}")
    print(f"Manifest: {manifest_path}")
    print(f"Metrics: {metrics_path}")
    LOGGER.info("Ingest finished. Metrics: %s", metrics_path)


def run_read(args: argparse.Namespace) -> None:
    settings = Settings.from_env()
    lake_root = Path(args.lake_root or settings.lake_root or DEFAULT_LAKE_ROOT).resolve()
    LOGGER.info("Starting read for format='%s'", args.format)
    t0 = time.perf_counter()

    if args.format in {"parquet", "delta"} and not args.uri:
        raise ValueError("--uri is required for parquet and delta reads")

    if args.format == "iceberg":
        if not args.table_name:
            raise ValueError("--table-name is required for iceberg reads")
        warehouse_uri = str((lake_root / args.prefix / "iceberg").resolve())
        df = read_iceberg(
            catalog_name=args.catalog_name,
            namespace=args.namespace,
            table_name=args.table_name,
            catalog_uri=settings.iceberg_catalog_uri,
            warehouse_uri=warehouse_uri,
        )
    elif args.format == "parquet":
        df = read_parquet(args.uri)
    else:
        df = read_delta(args.uri)

    elapsed = round(time.perf_counter() - t0, 4)
    preview = df.head(args.limit)
    print(f"rows={len(df)}, cols={len(df.columns)}")
    print(f"read_seconds={elapsed}")
    print(preview.to_markdown(index=False))
    LOGGER.info("Read finished in %ss (rows=%s cols=%s)", elapsed, len(df), len(df.columns))


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _configure_logging("INFO")

    if args.command == "ingest":
        run_ingest(args)
    elif args.command == "read":
        run_read(args)
    else:
        raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
