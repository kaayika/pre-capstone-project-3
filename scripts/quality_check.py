from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq


# ==========================================================
# Konfigurasi file dan jumlah data
# ==========================================================
RAW_FILE = Path(
    os.getenv(
        "RAW_FILE",
        "/opt/airflow/data_lake/raw/taxi_trips/taxi_trips.parquet",
    )
)

MAX_ROWS = int(
    os.getenv(
        "MAX_ROWS",
        "50000",
    )
)

RAW_REQUIRED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "total_amount",
}

# ==========================================================
# Konfigurasi PostgreSQL Taxi Warehouse
# ==========================================================
WAREHOUSE_HOST = os.getenv(
    "WAREHOUSE_HOST",
    "warehouse-db",
)

WAREHOUSE_PORT = int(
    os.getenv(
        "WAREHOUSE_PORT",
        "5432",
    )
)

WAREHOUSE_DB = os.getenv(
    "WAREHOUSE_DB",
    "taxi_warehouse",
)

WAREHOUSE_USER = os.getenv(
    "WAREHOUSE_USER",
    "taxi_user",
)

WAREHOUSE_PASSWORD = os.getenv(
    "WAREHOUSE_PASSWORD",
    "taxi_password",
)


def get_connection():
    """Membuat koneksi ke PostgreSQL Taxi Warehouse."""

    return psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB,
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
    )


def check_raw_file() -> None:
    """Memastikan file raw tersedia, valid, dan memiliki kolom minimum."""

    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Raw file tidak ditemukan: {RAW_FILE}"
        )

    file_size = RAW_FILE.stat().st_size

    if file_size <= 0:
        raise ValueError(
            f"Raw file kosong: {RAW_FILE}"
        )

    try:
        parquet_file = pq.ParquetFile(
            RAW_FILE
        )
    except Exception as error:
        raise ValueError(
            f"Raw file tidak dapat dibaca sebagai Parquet: {RAW_FILE}"
        ) from error

    raw_rows = parquet_file.metadata.num_rows

    if raw_rows <= 0:
        raise ValueError(
            "Raw file tidak memiliki baris data."
        )

    raw_columns = set(
        parquet_file.schema.names
    )

    missing_columns = sorted(
        RAW_REQUIRED_COLUMNS - raw_columns
    )

    if missing_columns:
        raise ValueError(
            "Kolom minimum tidak ditemukan pada raw file: "
            f"{missing_columns}"
        )

    print("=" * 60)
    print("QUALITY CHECK RAW BERHASIL")
    print(f"Raw file    : {RAW_FILE}")
    print(f"Size        : {file_size:,} bytes")
    print(f"Raw rows    : {raw_rows:,}")
    print("Format      : Parquet")
    print("Required col: lengkap")


def check_bronze_table() -> None:
    """Memastikan tabel bronze berisi data."""

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bronze.taxi_trips;
                """
            )

            bronze_rows = cursor.fetchone()[0]

    finally:
        connection.close()

    if bronze_rows <= 0:
        raise ValueError(
            "Tabel bronze.taxi_trips tidak memiliki data."
        )

    if bronze_rows > MAX_ROWS:
        raise ValueError(
            "Jumlah data bronze melebihi MAX_ROWS. "
            f"Bronze rows: {bronze_rows:,}, "
            f"MAX_ROWS: {MAX_ROWS:,}"
        )

    print("=" * 60)
    print("QUALITY CHECK BRONZE BERHASIL")
    print(f"Bronze rows : {bronze_rows:,}")
    print(f"Maximum     : {MAX_ROWS:,}")


def check_silver_quality() -> None:
    """Memastikan data silver bersih dan valid."""

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bronze.taxi_trips;
                """
            )

            bronze_rows = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS silver_rows,
                    COUNT(*) FILTER (
                        WHERE
                            pickup_datetime IS NULL
                            OR dropoff_datetime IS NULL
                            OR trip_distance IS NULL
                            OR total_amount IS NULL
                            OR trip_distance < 0
                            OR total_amount < 0
                            OR dropoff_datetime < pickup_datetime
                    ) AS invalid_rows
                FROM silver.taxi_trips_clean;
                """
            )

            silver_rows, invalid_rows = cursor.fetchone()

    finally:
        connection.close()

    if silver_rows <= 0:
        raise ValueError(
            "Tabel silver.taxi_trips_clean tidak memiliki data."
        )

    if silver_rows > bronze_rows:
        raise ValueError(
            "Jumlah data silver lebih besar dari bronze."
        )

    if invalid_rows > 0:
        raise ValueError(
            f"Ditemukan {invalid_rows:,} data invalid pada silver."
        )

    print("=" * 60)
    print("QUALITY CHECK SILVER BERHASIL")
    print(f"Bronze rows : {bronze_rows:,}")
    print(f"Silver rows : {silver_rows:,}")
    print(f"Invalid rows: {invalid_rows:,}")


def check_gold_output() -> None:
    """Memastikan tabel gold tersedia dan konsisten."""

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM silver.taxi_trips_clean;
                """
            )

            silver_rows = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS gold_rows,
                    COALESCE(
                        SUM(total_trips),
                        0
                    ) AS summarized_trips,
                    COUNT(*) FILTER (
                        WHERE
                            trip_date IS NULL
                            OR total_trips <= 0
                            OR total_revenue IS NULL
                            OR total_revenue < 0
                            OR average_revenue IS NULL
                            OR average_revenue < 0
                            OR total_trip_distance IS NULL
                            OR total_trip_distance < 0
                            OR average_trip_distance IS NULL
                            OR average_trip_distance < 0
                    ) AS invalid_gold_rows
                FROM gold.daily_taxi_summary;
                """
            )

            (
                gold_rows,
                summarized_trips,
                invalid_gold_rows,
            ) = cursor.fetchone()

    finally:
        connection.close()

    if gold_rows <= 0:
        raise ValueError(
            "Tabel gold.daily_taxi_summary tidak memiliki data."
        )

    if invalid_gold_rows > 0:
        raise ValueError(
            f"Ditemukan {invalid_gold_rows:,} data invalid pada gold."
        )

    if summarized_trips != silver_rows:
        raise ValueError(
            "Total trip pada gold tidak sama dengan jumlah data silver. "
            f"Silver rows: {silver_rows:,}, "
            f"Gold summarized trips: {summarized_trips:,}"
        )

    print("=" * 60)
    print("QUALITY CHECK GOLD BERHASIL")
    print(f"Silver rows      : {silver_rows:,}")
    print(f"Gold rows        : {gold_rows:,}")
    print(f"Summarized trips : {summarized_trips:,}")
    print(f"Invalid gold rows: {invalid_gold_rows:,}")


if __name__ == "__main__":
    check_raw_file()
    check_bronze_table()
    check_silver_quality()
    check_gold_output()

    print("=" * 60)
    print("SEMUA QUALITY CHECK BERHASIL")
