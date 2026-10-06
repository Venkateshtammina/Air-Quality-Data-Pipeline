import os

import psycopg2
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Load Curated Air Quality to PostgreSQL")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "software.amazon.awssdk:bundle:2.29.52,"
        "org.postgresql:postgresql:42.7.8"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.auth.profile.name",
        "default"
    )
    .config(
        "spark.driver.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .config(
        "spark.executor.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .getOrCreate()
)

S3_PATH = "s3a://agri-data-engineering/curated/air_quality/"

DB_HOST = "traffic_postgres"
DB_PORT = 5432
DB_NAME = "agri_data_db"
DB_USER = "traffic_user"
DB_PASSWORD = os.environ.get("AGRI_DB_PASSWORD")

STAGING_TABLE = "air_quality_readings_staging"
TARGET_TABLE = "air_quality_readings"

if not DB_PASSWORD:
    raise RuntimeError("AGRI_DB_PASSWORD environment variable is not set.")

jdbc_url = (
    f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/"
    f"{DB_NAME}?sslmode=disable"
)

connection_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

target_columns = [
    "station_id",
    "station_name",
    "city",
    "state",
    "timestamp",
    "temperature",
    "pm25",
    "pm10",
    "ozone",
    "co",
    "no2",
    "so2",
    "rainfall",
    "relative_humidity",
    "wind_direction",
    "wind_speed",
    "date",
    "year",
    "month",
    "day",
    "hour",
    "pm25_category",
]


try:
    print("Reading curated data from S3...")

    curated_df = spark.read.parquet(S3_PATH)

    row_count = curated_df.count()

    print(f"Rows read from S3: {row_count}")
    print(f"Target table: {TARGET_TABLE}")
    print(f"Columns in curated data: {len(curated_df.columns)}")

    load_df = curated_df.select(target_columns)

    print(f"Columns selected for PostgreSQL: {len(load_df.columns)}")

    print(f"Writing data to staging table: {STAGING_TABLE}")

    load_df.write.jdbc(
        url=jdbc_url,
        table=STAGING_TABLE,
        mode="overwrite",
        properties=connection_properties,
    )

    print("Staging table written successfully.")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        conn.autocommit = False

        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {TARGET_TABLE} (
                    station_id,
                    station_name,
                    city,
                    state,
                    timestamp,
                    temperature,
                    pm25,
                    pm10,
                    ozone,
                    co,
                    no2,
                    so2,
                    rainfall,
                    relative_humidity,
                    wind_direction,
                    wind_speed,
                    date,
                    year,
                    month,
                    day,
                    hour,
                    pm25_category
                )
                SELECT
                    station_id,
                    station_name,
                    city,
                    state,
                    timestamp,
                    temperature,
                    pm25,
                    pm10,
                    ozone,
                    co,
                    no2,
                    so2,
                    rainfall,
                    relative_humidity,
                    wind_direction,
                    wind_speed,
                    date,
                    year,
                    month,
                    day,
                    hour,
                    pm25_category
                FROM {STAGING_TABLE}
                ON CONFLICT (station_id, timestamp)
                DO NOTHING
                """
            )

            inserted_rows = cursor.rowcount

            cursor.execute(
                f"DROP TABLE IF EXISTS {STAGING_TABLE}"
            )

        conn.commit()

        print(f"Rows inserted into PostgreSQL: {inserted_rows}")
        print(f"PostgreSQL load status: SUCCESS")
        print(f"Staging table {STAGING_TABLE} dropped successfully.")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    print("PostgreSQL load completed successfully.")

finally:
    spark.stop()