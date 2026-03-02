#!/usr/bin/env python3
from __future__ import annotations

import ssl
from pathlib import Path
from urllib.request import urlopen

SOURCE_URL = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out_path = root / "dbt" / "trino_pipeline" / "seeds" / "titanic_dataset.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with urlopen(SOURCE_URL, timeout=60, context=_ssl_context()) as resp:
        payload = resp.read()

    out_path.write_bytes(payload)
    print(f"Downloaded {len(payload)} bytes -> {out_path}")


if __name__ == "__main__":
    main()
