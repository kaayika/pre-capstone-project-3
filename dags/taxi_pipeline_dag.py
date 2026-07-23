from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

from extract import extract_to_datalake
from load_to_postgres import load_to_postgres_bronze
from quality_check import (
    check_bronze_table,
    check_gold_output,
    check_raw_file,
    check_silver_quality,
)
from transform import (
    build_gold_mart,
    transform_to_silver,
)


# ==========================================================
# Definisi DAG
# ==========================================================
with DAG(
    dag_id="taxi_pipeline_dag",
    description=(
        "Pipeline Taxi dari local data lake "
        "ke PostgreSQL Bronze, Silver, dan Gold"
    ),
    schedule="@monthly",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Jakarta",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "kayika",
        "retries": 1,
    },
    tags=[
        "pre-capstone-3",
        "taxi",
        "etl",
    ],
) as dag:

    # ======================================================
    # Task 1 — Extract file ke raw data lake
    # ======================================================
    @task(task_id="extract_to_datalake")
    def extract_task() -> None:
        extract_to_datalake()


    # ======================================================
    # Task 2 — Quality check raw file
    # ======================================================
    @task(task_id="check_raw_file")
    def check_raw_task() -> None:
        check_raw_file()


    # ======================================================
    # Task 3 — Load data ke PostgreSQL Bronze
    # ======================================================
    @task(task_id="load_to_postgres_bronze")
    def load_bronze_task() -> None:
        load_to_postgres_bronze()


    # ======================================================
    # Task 4 — Quality check Bronze
    # ======================================================
    @task(task_id="check_bronze_table")
    def check_bronze_task() -> None:
        check_bronze_table()


    # ======================================================
    # Task 5 — Transform Bronze ke Silver
    # ======================================================
    @task(task_id="transform_to_silver")
    def transform_silver_task() -> None:
        transform_to_silver()


    # ======================================================
    # Task 6 — Quality check Silver
    # ======================================================
    @task(task_id="check_silver_quality")
    def check_silver_task() -> None:
        check_silver_quality()


    # ======================================================
    # Task 7 — Build Gold Mart
    # ======================================================
    @task(task_id="build_gold_mart")
    def build_gold_task() -> None:
        build_gold_mart()


    # ======================================================
    # Task 8 — Quality check Gold
    # ======================================================
    @task(task_id="check_gold_output")
    def check_gold_task() -> None:
        check_gold_output()


    # ======================================================
    # Membuat instance seluruh task
    # ======================================================
    extract = extract_task()
    check_raw = check_raw_task()
    load_bronze = load_bronze_task()
    check_bronze = check_bronze_task()
    transform_silver = transform_silver_task()
    check_silver = check_silver_task()
    build_gold = build_gold_task()
    check_gold = check_gold_task()


    # ======================================================
    # Urutan task pipeline
    # ======================================================
    (
        extract
        >> check_raw
        >> load_bronze
        >> check_bronze
        >> transform_silver
        >> check_silver
        >> build_gold
        >> check_gold
    )
