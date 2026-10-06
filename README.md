# Air Quality Data Pipeline

An end-to-end data engineering pipeline that ingests Bengaluru air-quality data from a public API, stores raw data in S3, transforms and validates it with PySpark, loads curated data into PostgreSQL, and builds analytical models with dbt. Apache Airflow orchestrates the workflow.

## Architecture

```text
Public API
    ↓
Python Ingestion
    ↓
S3 Raw Data
    ↓
PySpark Transformation
    ↓
Data Quality Gate
    ↓
S3 Curated Parquet
    ↓
PostgreSQL
    ↓
dbt Analytics Models

        ↑
   Apache Airflow
   Orchestration
```

## Tech Stack

- **Python** — API ingestion, retries, checkpointing and incremental processing
- **Amazon S3** — raw and curated data storage
- **PySpark** — transformation, cleaning and deduplication
- **PostgreSQL** — curated relational data store
- **dbt** — analytical transformations and data tests
- **Apache Airflow** — workflow orchestration and retries
- **Docker** — local infrastructure and Airflow environment
- **MinIO** — S3-compatible local object storage

## Pipeline

### 1. Ingestion

Fetches air-quality records from the public API using pagination and retry handling.

The ingestion layer also implements:

- Checkpoint-based processing
- Incremental reconciliation
- Duplicate detection
- Failure-safe checkpoint updates
- Idempotent processing

Raw JSON records are stored in S3.

### 2. Transformation

PySpark processes the raw data and:

- Parses timestamps
- Removes invalid records
- Deduplicates by `station_id + timestamp`
- Removes completely null columns
- Performs data validation
- Writes curated data as Parquet

### 3. Data Quality

The pipeline validates:

- Non-empty dataset
- Non-null station IDs and timestamps
- PM2.5 ≥ 0
- Wind speed ≥ 0
- Relative humidity between 0–100
- Wind direction between 0–360
- Unique `station_id + timestamp`

The quality gate runs before PostgreSQL loading.

### 4. PostgreSQL + dbt

Validated Parquet data is loaded into PostgreSQL through a staging table with conflict handling to maintain idempotency.

dbt then creates analytical models including:

- Daily air quality
- Hourly air quality
- Daily pollution mart

## Airflow DAG

```text
run_air_quality_ingestion
            ↓
transform_air_quality
            ↓
data_quality_gate
            ↓
load_air_quality_to_postgres
```

Tasks are configured with dependencies and retries so downstream processing only occurs after successful upstream execution.

## Results

- **70,176** valid air-quality records processed
- **70,176** unique `station_id + timestamp` records
- Data quality checks: **PASS**
- PostgreSQL load: **SUCCESS**
- dbt build: **22/22** checks and operations passed

## Project Structure

```text
Air-Quality-Data-Pipeline/
│
├── airflow/
│   ├── dags/
│   ├── Dockerfile
│   └── docker-compose.yaml
│
├── dbt/
│   └── agri_dbt/
│       └── models/
│
├── src/
│   ├── ingestion/
│   └── spark/
│
├── data/
├── logs/
├── .venv/
├── .gitignore
└── README.md
```

## Key Engineering Concepts

**API ingestion • S3 data lake • PySpark • Data quality • PostgreSQL • dbt • Airflow • Docker • Checkpointing • Incremental processing • Idempotency • Deduplication**