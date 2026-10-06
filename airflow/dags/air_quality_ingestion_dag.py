from datetime import datetime, timedelta

import os
import sys

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def run_air_quality_ingestion():
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

    os.environ["AWS_DEFAULT_REGION"] = "eu-north-1"

    sys.path.insert(0, "/opt/airflow/src/ingestion")

    from api_test import run_ingestion

    print("Starting Air Quality ingestion pipeline...")

    run_ingestion()

    print("Air Quality ingestion pipeline completed successfully.")


with DAG(
    dag_id="air_quality_ingestion",
    start_date=datetime(2026, 10, 6),
    schedule=None,
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

    postgres_load_task = BashOperator(
        task_id="load_air_quality_to_postgres",
        bash_command=(
            "python /opt/airflow/src/spark/load_curated_to_postgres.py"
        ),
    )

    ingestion_task >> spark_transform_task
    spark_transform_task >> data_quality_task
    data_quality_task >> postgres_load_task