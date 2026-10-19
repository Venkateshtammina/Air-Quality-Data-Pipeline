
from datetime import datetime, timedelta
import os
import sys
import subprocess
from urllib.parse import parse_qs, unquote, urlparse

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

def run_dbt_models():
    """Build and test the analytics models after the Neon batch load."""

    database_url = os.environ.get("NEON_DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "NEON_DATABASE_URL is not configured in the Airflow environment."
        )

    parsed_url = urlparse(database_url)
    if not parsed_url.hostname or not parsed_url.path.strip("/"):
        raise RuntimeError("NEON_DATABASE_URL is not a valid PostgreSQL URL.")

    query = parse_qs(parsed_url.query)
    dbt_environment = os.environ.copy()
    dbt_environment.update({
        "DBT_HOST": parsed_url.hostname,
        "DBT_USER": unquote(parsed_url.username or ""),
        "DBT_PASSWORD": unquote(parsed_url.password or ""),
        "DBT_PORT": str(parsed_url.port or 5432),
        "DBT_DBNAME": unquote(parsed_url.path.lstrip("/")),
        "DBT_SSLMODE": query.get("sslmode", ["require"])[0],
    })

    project_dir = "/opt/airflow/dbt/agri_dbt"
    profiles_dir = "/opt/airflow/dbt"

    subprocess.run(
        [
            "dbt", "run",
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
        ],
        check=True,
        env=dbt_environment,
    )
    subprocess.run(
        [
            "dbt", "test",
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
        ],
        check=True,
        env=dbt_environment,
    )


def verify_neon_data():
    """Confirm batch completion and the small shared time dimension."""

    spark_path = "/opt/airflow/src/spark"
    if spark_path not in sys.path:
        sys.path.insert(0, spark_path)

    from batch_state import get_pending_batch

    pending_batch = get_pending_batch()
    if pending_batch is not None:
        raise RuntimeError(
            "The transformed batch was not marked complete by the loader."
        )

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

            # The loader performs batch-scoped source/fact/dimension checks
            # in its transaction. This stable shared dimension is small.
            cursor.execute("""
                SELECT COUNT(*), COUNT(DISTINCT hour)
                FROM public.dim_time
            """)
            time_row_count, distinct_hours = cursor.fetchone()

    print("\n========== Incremental Load Verification ==========")
    print("Pending batch: none (loader committed and marked it complete)")
    print(f"Time dimension rows: {time_row_count}")
    print("===================================================")

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

    dbt_task = PythonOperator(
        task_id="run_and_test_dbt_models",
        python_callable=run_dbt_models,
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
        >> dbt_task
        >> neon_verification_task
    )
