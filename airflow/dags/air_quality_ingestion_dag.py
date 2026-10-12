
from datetime import datetime, timedelta
import os
import sys

import psycopg2

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


# ------------------------------------------------------------
# Ingestion
# ------------------------------------------------------------

def run_air_quality_ingestion():
    """Run ingestion using the configured Airflow AWS connection."""

    hook = AwsBaseHook(aws_conn_id="aws_default")
    credentials = hook.get_credentials()

    if credentials is None:
        raise RuntimeError(
            "Could not retrieve AWS credentials from aws_default."
        )

    os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key

    if credentials.token:
        os.environ["AWS_SESSION_TOKEN"] = credentials.token

    os.environ["AWS_REGION"] = "eu-north-1"
    os.environ["AWS_DEFAULT_REGION"] = "eu-north-1"

    ingestion_path = "/opt/airflow/src/ingestion"
    if ingestion_path not in sys.path:
        sys.path.insert(0, ingestion_path)

    from api_test import run_ingestion

    print("Starting Air Quality ingestion pipeline...")
    run_ingestion()
    print("Air Quality ingestion completed successfully.")


# ------------------------------------------------------------
# Incremental Neon and star-schema load
# ------------------------------------------------------------


def load_neon_incrementally():
    """Load curated S3 data into Neon and the star schema."""

    import os
    import sys
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    hook = AwsBaseHook(aws_conn_id="aws_default")
    credentials = hook.get_credentials()

    if credentials is None:
        raise RuntimeError(
            "Could not retrieve AWS credentials from aws_default."
        )

    # Provide credentials to Spark within this task.
    os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key

    if credentials.token:
        os.environ["AWS_SESSION_TOKEN"] = credentials.token
    else:
        os.environ.pop("AWS_SESSION_TOKEN", None)

    os.environ["AWS_REGION"] = "eu-north-1"
    os.environ["AWS_DEFAULT_REGION"] = "eu-north-1"

    sys.path.insert(0, "/opt/airflow/src/spark")
    from load_curated_to_neon import run_incremental_load

    print("Starting incremental Neon and star-schema loading...")
    run_incremental_load()
    print("Incremental Neon and star-schema loading completed.")



# ------------------------------------------------------------
# Final Neon verification
# ------------------------------------------------------------

def verify_neon_data():
    """Verify source, fact, and dimension consistency without inserts."""

    database_url = os.environ.get("NEON_DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "NEON_DATABASE_URL is not configured in the Airflow environment."
        )

    with psycopg2.connect(
        database_url,
        connect_timeout=30,
        application_name="air_quality_airflow_verification",
    ) as connection:
        with connection.cursor() as cursor:

            # Source-table count and uniqueness
            cursor.execute("""
                SELECT
                    COUNT(*),
                    MIN("timestamp"),
                    MAX("timestamp"),
                    COUNT(DISTINCT (station_id, "timestamp"))
                FROM public.air_quality_readings
            """)

            source_count, min_timestamp, max_timestamp, unique_keys = (
                cursor.fetchone()
            )

            # Fact-table count
            cursor.execute("""
                SELECT COUNT(*)
                FROM public.fact_air_quality
            """)
            fact_count = cursor.fetchone()[0]

            # Ensure every source row maps to all required dimensions.
            cursor.execute("""
                SELECT COUNT(*)
                FROM public.air_quality_readings r
                LEFT JOIN public.dim_station s
                    ON s.station_id = r.station_id
                LEFT JOIN public.dim_date d
                    ON d.full_date = r."timestamp"::date
                LEFT JOIN public.dim_time t
                    ON t.hour =
                       EXTRACT(HOUR FROM r."timestamp")::INTEGER
                WHERE s.station_key IS NULL
                   OR d.date_key IS NULL
                   OR t.time_key IS NULL
            """)
            missing_dimension_refs = cursor.fetchone()[0]

            # Ensure all fact rows reference existing dimensions.
            cursor.execute("""
                SELECT COUNT(*)
                FROM public.fact_air_quality f
                LEFT JOIN public.dim_station s
                    ON s.station_key = f.station_key
                LEFT JOIN public.dim_date d
                    ON d.date_key = f.date_key
                LEFT JOIN public.dim_time t
                    ON t.time_key = f.time_key
                WHERE s.station_key IS NULL
                   OR d.date_key IS NULL
                   OR t.time_key IS NULL
            """)
            invalid_fact_refs = cursor.fetchone()[0]

            # Check expected hourly dimension coverage.
            cursor.execute("""
                SELECT COUNT(*), COUNT(DISTINCT hour)
                FROM public.dim_time
            """)
            time_row_count, distinct_hours = cursor.fetchone()

    print("\n========== Neon Verification ==========")
    print(f"Source records: {source_count:,}")
    print(f"Fact records: {fact_count:,}")
    print(f"Unique source keys: {unique_keys:,}")
    print(f"Earliest timestamp: {min_timestamp}")
    print(f"Latest timestamp: {max_timestamp}")
    print(f"Source rows missing dimension matches: {missing_dimension_refs:,}")
    print(f"Facts with invalid dimension references: {invalid_fact_refs:,}")
    print(f"Time dimension rows: {time_row_count}")
    print("=======================================")

    if source_count == 0:
        raise RuntimeError("The Neon source table is empty.")

    if unique_keys != source_count:
        raise RuntimeError(
            "Source-table station/timestamp keys are not unique."
        )

    if fact_count != source_count:
        raise RuntimeError(
            f"Source/fact count mismatch: {source_count:,} source rows "
            f"versus {fact_count:,} fact rows."
        )

    if missing_dimension_refs != 0:
        raise RuntimeError(
            f"{missing_dimension_refs:,} source rows lack dimension matches."
        )

    if invalid_fact_refs != 0:
        raise RuntimeError(
            f"{invalid_fact_refs:,} fact rows have invalid dimension references."
        )

    if time_row_count != 24 or distinct_hours != 24:
        raise RuntimeError(
            "dim_time must contain all 24 distinct hours."
        )

    print("Neon verification PASSED.")


# ------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------

with DAG(
    dag_id="air_quality_ingestion",
    start_date=datetime(2026, 10, 6),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    ingestion_task = PythonOperator(
        task_id="run_air_quality_ingestion",
        python_callable=run_air_quality_ingestion,
    )

    spark_transform_task = BashOperator(
        task_id="transform_air_quality",
        bash_command=(
            "python /opt/airflow/src/spark/transform_curated.py"
        ),
    )

    data_quality_task = BashOperator(
        task_id="data_quality_gate",
        bash_command=(
            "python /opt/airflow/src/spark/run_data_quality.py"
        ),
    )

    incremental_neon_load_task = PythonOperator(
        task_id="incremental_neon_and_star_schema_load",
        python_callable=load_neon_incrementally,
    )

    neon_verification_task = PythonOperator(
        task_id="verify_neon_data",
        python_callable=verify_neon_data,
    )

    (
        ingestion_task
        >> spark_transform_task
        >> data_quality_task
        >> incremental_neon_load_task
        >> neon_verification_task
    )
