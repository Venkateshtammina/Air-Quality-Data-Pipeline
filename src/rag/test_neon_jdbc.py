
import os
from pathlib import Path
from urllib.parse import urlparse, unquote

from dotenv import load_dotenv
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

database_url = os.getenv("NEON_DATABASE_URL")

if not database_url:
    raise RuntimeError("NEON_DATABASE_URL is missing from .env")

parsed = urlparse(database_url)

jdbc_url = (
    f"jdbc:postgresql://{parsed.hostname}:"
    f"{parsed.port or 5432}{parsed.path}?sslmode=require"
)

properties = {
    "user": unquote(parsed.username or ""),
    "password": unquote(parsed.password or ""),
    "driver": "org.postgresql.Driver",
}

spark = (
    SparkSession.builder
    .appName("Test Neon JDBC Connection")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "software.amazon.awssdk:bundle:2.29.52,"
        "org.postgresql:postgresql:42.7.8",
    )
    .getOrCreate()
)

try:
    result = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "dbtable",
            "(SELECT current_database() AS db_name, "
            "current_user AS db_user) AS connection_check",
        )
        .options(**properties)
        .load()
    )

    row = result.first()

    print("Spark JDBC connection successful!")
    print(f"Database: {row['db_name']}")
    print(f"User: {row['db_user']}")

finally:
    spark.stop()
