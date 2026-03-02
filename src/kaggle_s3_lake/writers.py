from __future__ import annotations

from typing import Iterable

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


def write_parquet(df: pd.DataFrame, uri: str) -> None:
    df.to_parquet(uri, engine="pyarrow", index=False)


def write_delta(df: pd.DataFrame, uri: str) -> None:
    write_deltalake(uri, df, mode="overwrite")


def _iceberg_type_for_dtype(dtype: pd.api.extensions.ExtensionDtype):
    if pd.api.types.is_bool_dtype(dtype):
        return BooleanType()
    if pd.api.types.is_integer_dtype(dtype):
        return LongType()
    if pd.api.types.is_float_dtype(dtype):
        return DoubleType()
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return TimestampType()
    return StringType()


def _to_iceberg_schema(df: pd.DataFrame) -> Schema:
    fields: list[NestedField] = []
    for field_id, column in enumerate(df.columns, start=1):
        iceberg_type = _iceberg_type_for_dtype(df[column].dtype)
        fields.append(NestedField(field_id=field_id, name=str(column), field_type=iceberg_type, required=False))
    return Schema(*fields)


def write_iceberg(
    df: pd.DataFrame,
    *,
    catalog_name: str,
    namespace: str,
    table_name: str,
    catalog_uri: str,
    warehouse_uri: str,
) -> str:
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=catalog_uri,
        warehouse=warehouse_uri,
    )

    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    identifier: tuple[str, str] = (namespace, table_name)
    try:
        catalog.load_table(identifier)
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema = _to_iceberg_schema(df)
    table = catalog.create_table(identifier=identifier, schema=schema)

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    table.append(arrow_table)

    return f"{catalog_name}.{namespace}.{table_name}"


def dataframe_schema(df: pd.DataFrame) -> Iterable[dict[str, str]]:
    for col, dtype in df.dtypes.items():
        yield {"name": str(col), "dtype": str(dtype)}
