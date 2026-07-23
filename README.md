# Pre-Capstone Project 3

## Local Taxi Data Pipeline with Apache Airflow and PostgreSQL

**Nama:** Ni Nyoman Kayika Manuhita  
**Kelas:** Data Engineering (JCDEAH-009)  

---

## Project Overview

Project ini membangun data pipeline lokal untuk memproses dataset perjalanan taksi
dari file Parquet ke PostgreSQL.

Pipeline menggunakan Apache Airflow untuk mengatur proses extract, load,
transform, dan quality check secara berurutan.

Data diproses ke dalam tiga layer PostgreSQL:

- **Bronze** untuk menyimpan data mentah.
- **Silver** untuk menyimpan data yang sudah dibersihkan.
- **Gold** untuk menyimpan ringkasan perjalanan harian.

Pipeline dijalankan secara lokal menggunakan Docker Compose.

---

## Project Objectives

Tujuan project ini adalah:

1. Menyalin file sumber ke local data lake.
2. Memuat data Parquet ke PostgreSQL Bronze.
3. Membersihkan data Bronze menjadi Silver.
4. Membuat agregasi harian pada Gold.
5. Melakukan quality check pada setiap tahap pipeline.
6. Mengatur seluruh proses menggunakan Apache Airflow.
7. Memastikan pipeline dapat dijalankan ulang tanpa menggandakan data.

---

## Technologies

Teknologi yang digunakan:

- Python 3.11
- Apache Airflow 3.3.0
- PostgreSQL 16
- Docker
- Docker Compose
- Pandas
- PyArrow
- Psycopg2
- DBeaver
- SQL

---

## Pipeline Architecture

```text
Taxi Parquet Dataset
        |
        v
Local Data Lake
data_lake/raw/taxi_trips/taxi_trips.parquet
        |
        v
PostgreSQL Bronze
bronze.taxi_trips
        |
        v
PostgreSQL Silver
silver.taxi_trips_clean
        |
        v
PostgreSQL Gold
gold.daily_taxi_summary
        |
        v
Quality Checks
```

Apache Airflow menjalankan pipeline melalui DAG:

```text
extract_to_datalake
        |
        v
check_raw_file
        |
        v
load_to_postgres_bronze
        |
        v
check_bronze_table
        |
        v
transform_to_silver
        |
        v
check_silver_quality
        |
        v
build_gold_mart
        |
        v
check_gold_output
```

DAG ID:

```text
taxi_pipeline_dag
```

DAG menggunakan schedule bulanan (`@monthly`) dan tetap dapat dijalankan
secara manual melalui Airflow UI.

---

## Project Structure

```text
pre-capstone-project-3/
├── dags/
│   └── taxi_pipeline_dag.py
│
├── data_lake/
│   ├── raw/
│   │   └── taxi_trips/
│   │       └── taxi_trips.parquet      # generated locally
│   ├── staging/
│   └── processed/
│
├── scripts/
│   ├── extract.py
│   ├── load_to_postgres.py
│   ├── transform.py
│   └── quality_check.py
│
├── sql/
│   ├── 01_schema.sql
│   ├── 02_bronze_tables.sql
│   ├── 03_silver_transform.sql
│   └── 04_gold_mart.sql
│
├── source/
│   └── taxi_trips.parquet              # local only, not committed
│
├── logs/
├── plugins/
├── .env.example
├── .gitignore
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

> Folder `source/`, `data_lake/`, dan `logs/` tetap digunakan dalam project.
> File data besar seperti `.parquet`, hasil pipeline, dan log runtime tidak disimpan ke repository.
> Folder tersebut dapat dibuat ulang saat setup project mengikuti instruksi README.

> File `.env` dibuat secara lokal dari `.env.example` dan tidak disimpan ke repository.
---

## Main Components

### 1. Schema Initialization

File:

```text
sql/01_schema.sql
```

File ini membuat tiga schema PostgreSQL yang digunakan oleh pipeline:

- `bronze`
- `silver`
- `gold`

Schema dibuat menggunakan `CREATE SCHEMA IF NOT EXISTS`, sehingga aman
dijalankan ulang apabila schema sudah tersedia.

---

### 2. Bronze Table Definition

File:

```text
sql/02_bronze_tables.sql
```

File ini menghapus dan membuat ulang tabel:

```text
bronze.taxi_trips
```

Tabel Bronze terdiri dari 20 kolom yang menyesuaikan struktur data
NYC Yellow Taxi.

Strategi drop-and-recreate digunakan agar proses load dapat dijalankan
ulang tanpa menggandakan data.

---

### 3. Extract

File:

```text
scripts/extract.py
```

Fungsi:

```text
extract_to_datalake()
```

Proses ini menyalin file:

```text
source/taxi_trips.parquet
```

ke:

```text
data_lake/raw/taxi_trips/taxi_trips.parquet
```

---

### 4. Load to PostgreSQL Bronze

File:

```text
scripts/load_to_postgres.py
```

Fungsi:

```text
load_to_postgres_bronze()
```

Proses ini:

1. Membaca file Parquet dari raw data lake.
2. Membatasi jumlah data sesuai `MAX_ROWS`.
3. Menyesuaikan nama kolom.
4. Membuat ulang tabel Bronze.
5. Memuat data menggunakan PostgreSQL `COPY`.

Target:

```text
bronze.taxi_trips
```

---

### 5. Silver Transformation

File SQL:

```text
sql/03_silver_transform.sql
```

Target:

```text
silver.taxi_trips_clean
```

Proses cleaning Silver:

- menghapus data duplikat dengan `SELECT DISTINCT`;
- menghapus nilai tanggal pickup atau drop-off yang kosong;
- menghapus data dengan drop-off sebelum pickup;
- menghapus nilai `trip_distance` yang kosong atau negatif;
- menghapus nilai `total_amount` yang kosong atau negatif.

---

### 6. Gold Data Mart

File SQL:

```text
sql/04_gold_mart.sql
```

Target:

```text
gold.daily_taxi_summary
```

Gold menyimpan ringkasan berdasarkan tanggal pickup dengan kolom:

- `trip_date`
- `total_trips`
- `total_revenue`
- `average_revenue`
- `total_trip_distance`
- `average_trip_distance`

Satu baris pada Gold mewakili satu tanggal unik.

---

### 7. Quality Checks

File:

```text
scripts/quality_check.py
```

Quality check yang dilakukan:

#### Raw

- file tersedia;
- ukuran file lebih besar dari 0 byte;
- file dapat dibaca sebagai format Parquet;
- file memiliki jumlah baris lebih dari 0;
- kolom minimum yang diperlukan tersedia.

#### Bronze

- tabel berisi data;
- jumlah baris tidak melebihi `MAX_ROWS`.

#### Silver

- jumlah Silver tidak lebih besar dari Bronze;
- tidak terdapat data invalid;
- tidak terdapat perjalanan dengan drop-off sebelum pickup;
- tidak terdapat nilai jarak atau total pembayaran negatif.

#### Gold

- tabel Gold tidak kosong;
- tidak terdapat agregasi invalid;
- `SUM(total_trips)` pada Gold sama dengan jumlah baris Silver.

---

## Database Tables

| Layer | Table | Description |
|---|---|---|
| Bronze | `bronze.taxi_trips` | Data perjalanan taksi mentah |
| Silver | `silver.taxi_trips_clean` | Data yang sudah dibersihkan |
| Gold | `gold.daily_taxi_summary` | Ringkasan perjalanan per tanggal |

---

## Environment Configuration

Buat file `.env` dari template:

```bash
cp .env.example .env
```

Kemudian sesuaikan nilainya:

```env
AIRFLOW_UID=1000

WAREHOUSE_DB=taxi_warehouse
WAREHOUSE_USER=taxi_user
WAREHOUSE_PASSWORD=change_me
WAREHOUSE_PORT=5435

MAX_ROWS=50000
```

Untuk melihat UID Linux:

```bash
id -u
```

Nilai `AIRFLOW_UID` pada `.env` harus disesuaikan dengan hasil perintah tersebut.

File `.env` tidak disimpan ke GitHub karena dapat berisi credential.
Gunakan `.env.example` sebagai template konfigurasi.

---

## Source Data

Buat folder lokal yang diperlukan:

```bash
mkdir -p source
mkdir -p data_lake/raw/taxi_trips
mkdir -p data_lake/staging
mkdir -p data_lake/processed
mkdir -p logs
mkdir -p plugins
```

Letakkan file Parquet pada:

```text
source/taxi_trips.parquet
```

Pipeline mengharapkan file sumber menggunakan nama tersebut.

File sumber dan hasil local data lake tidak dimasukkan ke repository karena
folder `source/` dan `data_lake/` terdaftar pada `.gitignore`.

---

## How to Run

### 1. Clone atau buka project

```bash
cd pre-capstone-project-3
```

### 2. Buat file environment

```bash
cp .env.example .env
```

Edit konfigurasi:

```bash
nano .env
```

### 3. Buat folder lokal

```bash
mkdir -p source
mkdir -p data_lake/raw/taxi_trips
mkdir -p data_lake/staging
mkdir -p data_lake/processed
mkdir -p logs
mkdir -p plugins
```

### 4. Siapkan file source

Letakkan dataset pada:

```text
source/taxi_trips.parquet
```

Kemudian periksa:

```bash
ls -lh source/taxi_trips.parquet
```

### 5. Validasi Docker Compose

```bash
docker compose config
```

### 6. Build image

```bash
docker compose build
```

### 7. Jalankan container

```bash
docker compose up -d
```

### 8. Cek container

```bash
docker compose ps
```

Container yang diperlukan:

```text
precapstone-airflow
precapstone-airflow-db
precapstone-warehouse-db
```

### 9. Ambil password Airflow

```bash
docker compose exec airflow \
cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

### 10. Buka Airflow UI

```text
http://localhost:8084
```

Login menggunakan:

```text
Username: admin
Password: password yang dihasilkan Airflow
```

### 11. Jalankan DAG

Pada Airflow UI:

```text
Dags
→ taxi_pipeline_dag
→ Trigger
```

Tunggu sampai seluruh 8 task berstatus `Success`.

---

## Run Pipeline Manually

Pipeline juga dapat diuji tanpa trigger Airflow:

```bash
docker compose exec -T airflow python - <<'PY'
from extract import extract_to_datalake
from load_to_postgres import load_to_postgres_bronze
from transform import transform_to_silver, build_gold_mart
from quality_check import (
    check_raw_file,
    check_bronze_table,
    check_silver_quality,
    check_gold_output,
)

extract_to_datalake()
check_raw_file()

load_to_postgres_bronze()
check_bronze_table()

transform_to_silver()
check_silver_quality()

build_gold_mart()
check_gold_output()

print("PIPELINE TAXI BERHASIL DIJALANKAN")
PY
```

---

## PostgreSQL Connection

Gunakan konfigurasi berikut untuk membuka Taxi Warehouse melalui DBeaver:

```text
Host     : localhost
Port     : 5435
Database : taxi_warehouse
Username : taxi_user
Password : sesuai file .env
```

---

## Verification Queries

### Check row counts

```sql
SELECT
    (SELECT COUNT(*)
     FROM bronze.taxi_trips) AS bronze_rows,

    (SELECT COUNT(*)
     FROM silver.taxi_trips_clean) AS silver_rows,

    (SELECT COUNT(*)
     FROM gold.daily_taxi_summary) AS gold_rows,

    (SELECT COALESCE(SUM(total_trips), 0)
     FROM gold.daily_taxi_summary) AS gold_total_trips;
```

### Check invalid Silver rows

```sql
SELECT COUNT(*) AS invalid_silver_rows
FROM silver.taxi_trips_clean
WHERE
    pickup_datetime IS NULL
    OR dropoff_datetime IS NULL
    OR trip_distance IS NULL
    OR total_amount IS NULL
    OR trip_distance < 0
    OR total_amount < 0
    OR dropoff_datetime < pickup_datetime;
```

Expected result:

```text
invalid_silver_rows = 0
```

### Check Silver and Gold consistency

```sql
SELECT
    (SELECT COUNT(*)
     FROM silver.taxi_trips_clean) AS silver_rows,

    (SELECT COALESCE(SUM(total_trips), 0)
     FROM gold.daily_taxi_summary) AS summarized_trips,

    (SELECT COUNT(*)
     FROM silver.taxi_trips_clean)
    =
    (SELECT COALESCE(SUM(total_trips), 0)
     FROM gold.daily_taxi_summary) AS is_consistent;
```

Expected result:

```text
is_consistent = true
```

### Check Gold output

```sql
SELECT
    trip_date,
    total_trips,
    total_revenue,
    average_revenue,
    total_trip_distance,
    average_trip_distance
FROM gold.daily_taxi_summary
ORDER BY trip_date;
```

---

## Sample Pipeline Result

Contoh hasil dari pemrosesan maksimum 50.000 baris:

| Check | Result |
|---|---:|
| Bronze rows | 50,000 |
| Silver rows | 49,031 |
| Rows removed during Silver cleaning | 969 |
| Gold rows | 2 |
| Gold summarized trips | 49,031 |
| Invalid Silver rows | 0 |
| Invalid Gold rows | 0 |

Nilai Gold rows sebanyak 2 berarti 50.000 baris pertama pada file sumber
hanya mencakup dua tanggal pickup yang berbeda.

Jumlah data dapat berubah apabila file sumber atau nilai `MAX_ROWS` diubah.

---

## Idempotency Strategy

Pipeline dirancang agar idempotent.

Setiap pipeline dijalankan ulang:

1. Tabel Bronze dihapus dan dibuat kembali sebelum proses load.
2. Tabel Silver dihapus dan dibuat kembali sebelum transformasi.
3. Tabel Gold dihapus dan dibuat kembali sebelum agregasi.

Dengan strategi tersebut, menjalankan DAG lebih dari satu kali tidak
menambahkan data duplikat ke tabel yang sudah ada.

Contoh hasil pengujian:

```text
Bronze before = 50,000
Bronze after  = 50,000

Silver before = 49,031
Silver after  = 49,031

Gold before   = 2
Gold after    = 2
```

Data tidak berubah menjadi dua kali lipat setelah DAG dijalankan ulang.

---

## Check Airflow DAG

### Check import errors

```bash
docker compose exec airflow \
airflow dags list-import-errors --output=json
```

Expected result:

```json
[]
```

### Check registered DAG

```bash
docker compose exec airflow \
airflow dags list --output=table
```

### Check task list

```bash
docker compose exec airflow \
airflow tasks list taxi_pipeline_dag
```

DAG harus memiliki 8 task.

### Check DAG runs

```bash
docker compose exec airflow \
airflow dags list-runs taxi_pipeline_dag \
--output=table
```

Run terakhir harus memiliki state:

```text
success
```

---

## Assumptions and Limitations

1. File input menggunakan format Parquet.
2. Nama file input adalah `taxi_trips.parquet`.
3. Kolom file sumber mengikuti struktur NYC Yellow Taxi.
4. Pipeline memproses maksimal jumlah baris yang ditentukan oleh `MAX_ROWS`.
5. Pipeline dijalankan pada lingkungan lokal menggunakan Docker Compose.
6. DAG menggunakan schedule bulanan (`@monthly`) dan tetap dapat dijalankan
   secara manual melalui Airflow UI.
7. Folder `staging` dan `processed` belum digunakan pada implementasi saat ini.
8. Pipeline menggunakan strategi drop-and-recreate untuk menjaga idempotency.
9. File `.env` tidak disimpan di repository.
10. Dataset dan hasil local data lake tidak dimasukkan ke GitHub.
11. Setiap scheduled run memproses ulang file `source/taxi_trips.parquet`
    yang tersedia saat DAG dijalankan. Pipeline belum menggunakan parameter
    periode atau file bulanan dinamis.

---

## Stop the Project

Untuk menghentikan container:

```bash
docker compose down
```

Untuk menghentikan container sekaligus menghapus volume database:

```bash
docker compose down -v
```

> Gunakan `docker compose down -v` hanya jika ingin reset total.
> Perintah ini akan menghapus data PostgreSQL, termasuk table Bronze, Silver, dan Gold.

---

## Author

**Ni Nyoman Kayika Manuhita**  
**Data Engineering (JCDEAH-009)**
