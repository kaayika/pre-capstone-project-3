from __future__ import annotations

import io
import os
from pathlib import Path

import pandas as pd
import psycopg2


# ==========================================================
# Konfigurasi file
# ==========================================================
RAW_FILE = Path(
    os.getenv(
        "RAW_FILE",
        "/opt/airflow/data_lake/raw/taxi_trips/taxi_trips.parquet",
    )
)

SQL_DIR = Path(
    os.getenv(
        "SQL_DIR",
        "/opt/airflow/sql",
    )
)

MAX_ROWS = int(
    os.getenv(
        "MAX_ROWS",
        "50000",
    )
)


# ==========================================================
# Konfigurasi PostgreSQL
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


# ==========================================================
# Penyesuaian nama kolom Parquet
# ==========================================================
COLUMN_MAPPING = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
    "CBD_congestion_fee": "cbd_congestion_fee",
}


# Urutan harus sama dengan tabel bronze.taxi_trips
EXPECTED_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]


def get_connection():
    """Membuat koneksi ke PostgreSQL Taxi Warehouse."""

    return psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB,
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
    )


def read_sql_file(filename: str) -> str:
    """Membaca isi file SQL."""

    sql_path = SQL_DIR / filename

    if not sql_path.exists():
        raise FileNotFoundError(
            f"File SQL tidak ditemukan: {sql_path}"
        )

    return sql_path.read_text(
        encoding="utf-8",
    )


def prepare_dataframe() -> pd.DataFrame:
    """Membaca dan menyiapkan data Parquet untuk bronze."""

    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Raw file tidak ditemukan: {RAW_FILE}"
        )

    dataframe = pd.read_parquet(
        RAW_FILE,
    )

    dataframe = dataframe.head(
        MAX_ROWS
    ).copy()

    dataframe = dataframe.rename(
        columns=COLUMN_MAPPING
    )

    missing_columns = [
        column
        for column in EXPECTED_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Kolom berikut tidak ditemukan pada file Parquet: "
            f"{missing_columns}"
        )

    dataframe = dataframe[
        EXPECTED_COLUMNS
    ]

    dataframe["pickup_datetime"] = pd.to_datetime(
        dataframe["pickup_datetime"],
        errors="coerce",
    )

    dataframe["dropoff_datetime"] = pd.to_datetime(
        dataframe["dropoff_datetime"],
        errors="coerce",
    )

    return dataframe


def load_to_postgres_bronze() -> None:
    """Memuat data raw Parquet ke PostgreSQL bronze."""

    dataframe = prepare_dataframe()

    schema_sql = read_sql_file(
        "01_schema.sql"
    )

    bronze_table_sql = read_sql_file(
        "02_bronze_tables.sql"
    )

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            # Membuat schema jika belum tersedia
            cursor.execute(
                schema_sql
            )

            # Menghapus dan membuat ulang tabel bronze
            cursor.execute(
                bronze_table_sql
            )

            csv_buffer = io.StringIO()

            dataframe.to_csv(
                csv_buffer,
                index=False,
                header=False,
                na_rep="",
            )

            csv_buffer.seek(0)

            column_list = ", ".join(
                EXPECTED_COLUMNS
            )

            copy_sql = f"""
                COPY bronze.taxi_trips ({column_list})
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    HEADER FALSE,
                    NULL ''
                );
            """

            cursor.copy_expert(
                copy_sql,
                csv_buffer,
            )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bronze.taxi_trips;
                """
            )

            bronze_rows = cursor.fetchone()[0]

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    print("=" * 60)
    print("LOAD BRONZE BERHASIL")
    print(f"Raw file     : {RAW_FILE}")
    print(f"Maximum rows : {MAX_ROWS:,}")
    print(f"Bronze rows  : {bronze_rows:,}")
    print("Target table : bronze.taxi_trips")


if __name__ == "__main__":
    load_to_postgres_bronze()
