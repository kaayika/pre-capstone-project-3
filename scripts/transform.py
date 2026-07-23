from __future__ import annotations

import os
from pathlib import Path

import psycopg2


# ==========================================================
# Lokasi file SQL
# ==========================================================
SQL_DIR = Path(
    os.getenv(
        "SQL_DIR",
        "/opt/airflow/sql",
    )
)


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


def transform_to_silver() -> None:
    """Membersihkan data bronze dan membuat tabel silver."""

    schema_sql = read_sql_file(
        "01_schema.sql"
    )

    silver_sql = read_sql_file(
        "03_silver_transform.sql"
    )

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                schema_sql
            )

            cursor.execute(
                silver_sql
            )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM silver.taxi_trips_clean;
                """
            )

            silver_rows = cursor.fetchone()[0]

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    print("=" * 60)
    print("TRANSFORM SILVER BERHASIL")
    print(f"Silver rows  : {silver_rows:,}")
    print("Target table : silver.taxi_trips_clean")


def build_gold_mart() -> None:
    """Membuat daily taxi summary pada schema gold."""

    schema_sql = read_sql_file(
        "01_schema.sql"
    )

    gold_sql = read_sql_file(
        "04_gold_mart.sql"
    )

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                schema_sql
            )

            cursor.execute(
                gold_sql
            )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM gold.daily_taxi_summary;
                """
            )

            gold_rows = cursor.fetchone()[0]

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    print("=" * 60)
    print("BUILD GOLD BERHASIL")
    print(f"Gold rows    : {gold_rows:,}")
    print("Target table : gold.daily_taxi_summary")


if __name__ == "__main__":
    transform_to_silver()
    build_gold_mart()
