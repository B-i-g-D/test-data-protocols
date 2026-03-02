from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    lake_root: str
    iceberg_catalog_uri: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            lake_root=os.getenv("LAKE_ROOT", "./data_lake"),
            iceberg_catalog_uri=os.getenv("ICEBERG_CATALOG_URI", "sqlite:///./iceberg_catalog.db"),
        )
