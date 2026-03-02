from __future__ import annotations

import ssl
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from pathlib import Path

import certifi
import pandas as pd

SUPPORTED_SUFFIXES = {".csv", ".tsv", ".parquet", ".json"}


def download_dataset(dataset: str, output_dir: Path) -> list[Path]:
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset=dataset, path=str(output_dir), unzip=True, quiet=False)

    files = [
        p
        for p in output_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED_SUFFIXES
    ]
    if not files:
        raise FileNotFoundError(
            f"No supported tabular files were found in dataset '{dataset}'. Supported: {sorted(SUPPORTED_SUFFIXES)}"
        )
    return sorted(files)


def download_file_from_url(file_url: str, output_dir: Path) -> Path:
    parsed = urlparse(file_url)
    filename = Path(parsed.path).name or "dataset.csv"
    target = output_dir / filename
    output_dir.mkdir(parents=True, exist_ok=True)
    request = Request(file_url, headers={"User-Agent": "kaggle-s3-lake/0.1.0"})
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    with urlopen(request, context=ssl_context) as response:
        target.write_bytes(response.read())
    return target


def load_dataframe(files: list[Path], preferred_file: str | None = None) -> tuple[pd.DataFrame, Path]:
    file_path: Path
    if preferred_file:
        matches = [p for p in files if p.name == preferred_file or str(p).endswith(preferred_file)]
        if not matches:
            options = "\n".join(f"- {p.name}" for p in files)
            raise FileNotFoundError(f"File '{preferred_file}' not found. Available files:\n{options}")
        file_path = matches[0]
    else:
        file_path = files[0]

    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(file_path)
    elif suffix == ".tsv":
        df = pd.read_csv(file_path, sep="\t")
    elif suffix == ".parquet":
        df = pd.read_parquet(file_path)
    elif suffix == ".json":
        df = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported input suffix: {suffix}")

    return df, file_path
