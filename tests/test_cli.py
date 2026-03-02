from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from kaggle_s3_lake.cli import _build_parser, _dir_size_bytes, run_ingest, run_read
from kaggle_s3_lake.config import Settings


def _args_ingest(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        source_url="https://example.com/titanic.csv",
        dataset_id="titanic",
        lake_root=str(tmp_path / "lake"),
        prefix="kaggle-datasets",
        namespace="kaggle",
        table_name=None,
        catalog_name="local",
        sample_limit=None,
    )


def _args_read() -> argparse.Namespace:
    return argparse.Namespace(
        format="parquet",
        uri="/tmp/data.parquet",
        limit=5,
        catalog_name="local",
        namespace="kaggle",
        table_name=None,
        lake_root="./data_lake",
        prefix="kaggle-datasets",
    )


def test_dir_size_bytes(tmp_path: Path) -> None:
    root = tmp_path / "a"
    root.mkdir()
    (root / "x.bin").write_bytes(b"1234")
    sub = root / "sub"
    sub.mkdir()
    (sub / "y.bin").write_bytes(b"abc")

    assert _dir_size_bytes(root) == 7
    assert _dir_size_bytes(root / "x.bin") == 4
    assert _dir_size_bytes(tmp_path / "missing") == 0


def test_run_ingest_writes_manifest_metrics_and_logs(tmp_path: Path, monkeypatch, caplog) -> None:
    caplog.set_level("INFO")
    args = _args_ingest(tmp_path)

    settings = Settings(lake_root=str(tmp_path / "lake"), iceberg_catalog_uri=f"sqlite:///{tmp_path}/cat.db")
    monkeypatch.setattr("kaggle_s3_lake.cli.Settings.from_env", lambda: settings)

    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    fake_file = tmp_path / "download" / "file.csv"
    fake_file.parent.mkdir(parents=True, exist_ok=True)
    fake_file.write_text("id,name\n1,a\n2,b\n", encoding="utf-8")

    monkeypatch.setattr("kaggle_s3_lake.cli.download_file_from_url", lambda source_url, out: fake_file)
    monkeypatch.setattr("kaggle_s3_lake.cli.load_dataframe", lambda files, preferred_file=None: (df, fake_file))

    def fake_write_parquet(local_df: pd.DataFrame, uri: str) -> None:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_bytes(b"parquet-bytes")

    def fake_write_delta(local_df: pd.DataFrame, uri: str) -> None:
        d = Path(uri)
        d.mkdir(parents=True, exist_ok=True)
        (d / "_delta_log.json").write_text("{}", encoding="utf-8")

    def fake_write_iceberg(
        local_df: pd.DataFrame,
        *,
        catalog_name: str,
        namespace: str,
        table_name: str,
        catalog_uri: str,
        warehouse_uri: str,
    ) -> str:
        w = Path(warehouse_uri)
        w.mkdir(parents=True, exist_ok=True)
        (w / "metadata.json").write_text("{}", encoding="utf-8")
        return f"{catalog_name}.{namespace}.{table_name}"

    monkeypatch.setattr("kaggle_s3_lake.cli.write_parquet", fake_write_parquet)
    monkeypatch.setattr("kaggle_s3_lake.cli.write_delta", fake_write_delta)
    monkeypatch.setattr("kaggle_s3_lake.cli.write_iceberg", fake_write_iceberg)

    run_ingest(args)

    dataset_key = args.dataset_id.replace("/", "__")
    root = Path(args.lake_root) / args.prefix / dataset_key
    manifest_path = root / "manifest.json"
    metrics_path = root / "metrics.json"

    assert manifest_path.exists()
    assert metrics_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert manifest["dataset"] == args.dataset_id
    assert manifest["artifacts"]["parquet"].endswith("/parquet/data.parquet")
    assert manifest["read_hints"]["python_iceberg"].find("--lake-root") != -1

    assert metrics["event"] == "ingest"
    assert metrics["rows"] == 2
    assert "write_parquet_seconds" in metrics["steps"]
    assert "write_delta_seconds" in metrics["steps"]
    assert "write_iceberg_seconds" in metrics["steps"]

    log_text = "\n".join(caplog.messages)
    assert "Starting ingest" in log_text
    assert "Ingest finished" in log_text


def test_run_read_prints_read_metrics(monkeypatch, capsys) -> None:
    args = _args_read()
    monkeypatch.setattr("kaggle_s3_lake.cli.Settings.from_env", lambda: Settings(lake_root="./data_lake", iceberg_catalog_uri="sqlite:///./cat.db"))
    monkeypatch.setattr("kaggle_s3_lake.cli.read_parquet", lambda uri: pd.DataFrame({"a": [1, 2, 3]}))

    run_read(args)
    out = capsys.readouterr().out

    assert "rows=3, cols=1" in out
    assert "read_seconds=" in out


def test_ingest_parser_has_defaults() -> None:
    parser = _build_parser()
    args = parser.parse_args(["ingest"])
    assert args.source_url.startswith("https://")
    assert args.dataset_id == "titanic"
    assert args.prefix == "kaggle-datasets"
