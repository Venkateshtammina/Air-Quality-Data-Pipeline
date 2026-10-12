# Air Quality Data Pipeline with RAG-Powered Text-to-SQL

An end-to-end data engineering project that ingests Bengaluru air-quality data from the OpenCity API, stores raw and curated data in Amazon S3, transforms it with PySpark, loads it into PostgreSQL, and models it with dbt. Apache Airflow orchestrates the pipeline.

The project also includes a RAG-powered text-to-SQL assistant that converts natural-language questions into validated, read-only SQL queries against the air-quality warehouse.

## Architecture

### Data Pipeline

```text
OpenCity Air Quality API
          |
          v
Python Ingestion (pagination, retries, checkpoints)
          |
          v
Amazon S3 — Raw JSON
          |
          v
PySpark Transformations
          |
          v
Data Quality Validation
          |
          v
Amazon S3 — Curated Parquet
          |
          v
PostgreSQL (Neon) — Star Schema
          |
          v
dbt Models and Data Marts
```

### Text-to-SQL Assistant

```text
Natural Language Question
          |
          v
Semantic Retrieval (schema context)
          |
          v
Gemini SQL Generation
          |
          v
SQLGlot Validation
          |
          v
Read-Only PostgreSQL Execution
          |
          v
Results and Explanation
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Ingestion, pipeline logic, RAG application |
| Amazon S3 | Raw JSON and curated Parquet storage |
| PySpark | Transformation and data quality validation |
| PostgreSQL (Neon) | Analytical data warehouse |
| dbt | SQL transformations and data marts |
| Apache Airflow, Docker | Workflow orchestration |
| Sentence Transformers, scikit-learn | Embeddings and cosine-similarity retrieval |
| Gemini API | SQL generation and result explanations |
| SQLGlot | SQL parsing and validation |

## Data Pipeline

### Ingestion

Python retrieves records from the OpenCity API and writes the raw JSON to Amazon S3. The ingestion layer supports:

- Paginated API extraction
- Retries for transient failures
- Checkpoint-based recovery with failure-safe checkpoint updates
- Incremental ingestion and reconciliation
- Duplicate detection and idempotent runs

### Transformation and Data Quality

PySpark standardizes timestamps, removes invalid and duplicate records, and writes curated Parquet data to Amazon S3. A quality gate blocks downstream tasks if any of the following validations fail:

- Dataset is not empty
- Required station and timestamp fields are present
- PM2.5 and wind speed are non-negative
- Relative humidity is between 0 and 100
- Wind direction is in the range [0, 360)
- Station and timestamp combinations are unique

### Data Warehouse

Curated data is loaded into PostgreSQL (Neon) using a star schema:

| Table | Purpose |
|---|---|
| `fact_air_quality` | Air-quality measurements |
| `dim_date` | Calendar attributes |
| `dim_time` | Time attributes |
| `dim_station` | Monitoring station attributes |

### dbt Models

- Staging of air-quality readings
- Hourly and daily aggregations
- Incremental air-quality processing
- Daily pollution mart

### Airflow Orchestration

The main DAG runs the following tasks in order, so each step executes only after its predecessor succeeds:

```text
run_air_quality_ingestion → transform_air_quality → data_quality_gate → load_air_quality_to_postgres
```

## Text-to-SQL Assistant

1. **Retrieve:** Sentence Transformers embeddings and cosine similarity select relevant schema information and query guidance.
2. **Generate:** Gemini produces SQL from the retrieved context.
3. **Validate:** SQLGlot checks the generated query.
4. **Execute:** Approved queries run against PostgreSQL in a read-only transaction.
5. **Explain:** Results are presented in natural language.

### Safety Controls

- Only approved read-only query types are executed
- Multiple statements and disallowed operations are rejected
- Queries are limited to approved air-quality tables
- Read-only database transactions with a 15-second statement timeout
- Fallback to raw database results if answer generation fails
- Out-of-scope questions are declined

> **Note:** The retrieval knowledge base is held in memory; a persistent vector database is not used.

## Results

| Metric | Result |
|---|---:|
| Valid air-quality records | 70,176 |
| Unique station-timestamp combinations | 70,176 |
| Data coverage | January 2024 – December 2025 |
| Monitoring stations | 1 |
| dbt checks and operations | 22/22 passed |
| SQL validator checks | 7 passed |
| Query engine unit tests | 6 passed |
| Retriever unit tests | 3 passed |

## Project Structure

```text
Air-Quality-Data-Pipeline/
├── src/
│   ├── ingestion/        # API client, checkpoints, deduplication, S3 storage
│   ├── spark/            # Transformations, data quality, PostgreSQL load
│   └── rag/              # Retriever, SQL generator, validator, query engine
├── airflow/
│   ├── dags/
│   ├── config/
│   ├── Dockerfile
│   └── docker-compose.yaml
├── dbt/
│   └── agri_dbt/
│       └── models/
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.11+
- Java (compatible with your PySpark version)
- Docker Desktop
- An Amazon S3 bucket and configured AWS credentials
- A PostgreSQL database (e.g., Neon)
- A Gemini API key

### Installation

```bash
git clone <your-repository-url>
cd Air-Quality-Data-Pipeline

python -m venv .venv
.\.venv\Scripts\Activate.ps1      # Windows PowerShell
# source .venv/bin/activate       # macOS / Linux

python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root (never commit it):

```dotenv
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-3.1-flash-lite
DATABASE_URL=your_neon_postgresql_connection_url
```

AWS credentials and S3 settings are also required for the ingestion and Spark stages.

### Usage

Start Airflow:

```bash
docker compose -f airflow/docker-compose.yaml up -d
```

Run the text-to-SQL assistant:

```bash
python -m src.rag.main
```

Run the tests:

```bash
python -m src.rag.test_sql_validator
python -m src.rag.test_query_engine
python -m src.rag.test_retriever
```

## Author

**Venkatesh T** — Data Engineer