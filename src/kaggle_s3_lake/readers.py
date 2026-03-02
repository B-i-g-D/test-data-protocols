from __future__ import annotations

import pandas as pd
from deltalake import DeltaTable
from pyiceberg.catalog import load_catalog


def read_parquet(uri: str) -> pd.DataFrame:
    return pd.read_parquet(uri, engine="pyarrow")


def read_delta(uri: str) -> pd.DataFrame:
    table = DeltaTable(uri)
    return table.to_pyarrow_table().to_pandas()


def read_iceberg(
    *,
    catalog_name: str,
    namespace: str,
    table_name: str,
    catalog_uri: str,
    warehouse_uri: str,
) -> pd.DataFrame:
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=catalog_uri,
        warehouse=warehouse_uri,
    )
    table = catalog.load_table((namespace, table_name))
    return table.scan().to_arrow().to_pandas()
