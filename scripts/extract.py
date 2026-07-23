from __future__ import annotations

import os
import shutil
from pathlib import Path


SOURCE_FILE = Path(
    os.getenv(
        "SOURCE_FILE",
        "/opt/airflow/source/taxi_trips.parquet",
    )
)

RAW_FILE = Path(
    os.getenv(
        "RAW_FILE",
        "/opt/airflow/data_lake/raw/taxi_trips/taxi_trips.parquet",
    )
)


def extract_to_datalake() -> None:
    """Menyalin file sumber ke raw data lake."""

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source file tidak ditemukan: {SOURCE_FILE}"
        )

    RAW_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    shutil.copy2(
        SOURCE_FILE,
        RAW_FILE,
    )

    print("=" * 60)
    print("EXTRACT BERHASIL")
    print(f"Source : {SOURCE_FILE}")
    print(f"Target : {RAW_FILE}")
    print(f"Size   : {RAW_FILE.stat().st_size:,} bytes")
