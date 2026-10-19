
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType
from batch_state import complete_pending_batch, get_pending_batch


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

S3_PATH = "s3a://agri-data-engineering/curated/air_quality/"

EXPECTED_RECORDS = 70176
EXPECTED_MIN_TIMESTAMP = datetime(2024, 1, 1, 0, 0, 0)
EXPECTED_MAX_TIMESTAMP = datetime(2025, 12, 31, 23, 45, 0)

BATCH_SIZE = 5000

COLUMNS = [
    "city",
    "state",
    "station_id",
    "station_name",
    "timestamp",
    "date",
    "day",
    "hour",
    "pm25",
    "pm10",
    "temperature",
    "pressure",
    "relative_humidity",
    "wind_speed",
    "wind_direction",
    "rainfall",
    "total_rainfall",
    "solar_radiation",
    "co",
    "no",
    "no2",
    "nox",
    "ozone",
    "so2",
    "benzene",
    "toluene",
    "ethylbenzene",
    "mp_xylene",
    "nh3",
    "pm25_category",
    "year",
    "month",
]


# ------------------------------------------------------------
# Original full-load SQL
# ------------------------------------------------------------

BULK_INSERT_SQL = """
    INSERT INTO public.air_quality_readings (
        city, state, station_id, station_name, "timestamp", date,
        day, hour, pm25, pm10, temperature, pressure,
        relative_humidity, wind_speed, wind_direction, rainfall,
        total_rainfall, solar_radiation, co, no, no2, nox, ozone,
        so2, benzene, toluene, ethylbenzene, mp_xylene, nh3,
        pm25_category, year, month
    )
    VALUES %s
"""


# ------------------------------------------------------------
# Database configuration
# ------------------------------------------------------------

def get_database_url():
    """Read the Neon PostgreSQL connection URL."""

    database_url = os.getenv("NEON_DATABASE_URL")

    if not database_url:
        raise ValueError(
            "NEON_DATABASE_URL was not found in the environment "
            "or project .env file."
        )

    return database_url


# ------------------------------------------------------------
# Spark session
# ------------------------------------------------------------

def create_spark_session():
    """Create Spark with S3 access enabled."""

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    aws_region = os.getenv("AWS_REGION", "eu-north-1")
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
            "must be configured in the environment."
        )

    builder = (
        SparkSession.builder
        .appName("AirQualityCuratedToNeon")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "software.amazon.awssdk:bundle:2.29.52",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
    )

    if session_token:
        builder = builder.config(
            "spark.hadoop.fs.s3a.session.token",
            session_token,
        )

    if s3_endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(s3_endpoint.startswith("https://")).lower(),
            )
        )

    return builder.getOrCreate()


# ------------------------------------------------------------
# Shared curated-data validation
# ------------------------------------------------------------

def _read_curated_data(spark, path=S3_PATH):
    """Read the curated Parquet dataset and validate its structure."""

    print(f"\nReading curated data from: {path}")

    df = spark.read.parquet(path)

    missing_columns = sorted(set(COLUMNS) - set(df.columns))
    if missing_columns:
        raise ValueError(
            f"Curated data is missing required columns: {missing_columns}"
        )

    df = df.select(*COLUMNS)

    timestamp_type = df.schema["timestamp"].dataType
    if not isinstance(timestamp_type, TimestampNTZType):
        raise TypeError(
            "Expected timestamp to be Spark TimestampNTZType, "
            f"but found {timestamp_type}."
        )

    if df.filter(
        F.col("station_id").isNull()
        | F.col("timestamp").isNull()
    ).limit(1).count():
        raise ValueError(
            "Curated data contains null station_id or timestamp values."
        )

    if (
        df.groupBy("station_id", "timestamp")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError(
            "Curated data contains duplicate station_id/timestamp keys."
        )

    record_count = df.count()
    timestamp_stats = df.agg(
        F.min("timestamp").alias("min_timestamp"),
        F.max("timestamp").alias("max_timestamp"),
    ).first()

    print(f"Curated rows: {record_count:,}")
    print(f"Earliest timestamp: {timestamp_stats['min_timestamp']}")
    print(f"Latest timestamp:   {timestamp_stats['max_timestamp']}")
    print("Curated structure and business-key checks PASSED.")

    return df, record_count, timestamp_stats


def read_and_validate_curated_data(spark):
    """Strict validation for the original one-time full load."""

    df, record_count, stats = _read_curated_data(spark)

    if record_count != EXPECTED_RECORDS:
        raise ValueError(
            f"Expected {EXPECTED_RECORDS:,} records, "
            f"but found {record_count:,}."
        )

    if stats["min_timestamp"] != EXPECTED_MIN_TIMESTAMP:
        raise ValueError(
            f"Unexpected earliest timestamp: {stats['min_timestamp']}"
        )

    if stats["max_timestamp"] != EXPECTED_MAX_TIMESTAMP:
        raise ValueError(
            f"Unexpected latest timestamp: {stats['max_timestamp']}"
        )

    print("Full-load validation PASSED.")
    return df


def read_incremental_curated_data(spark, path):
    """Validation for recurring loading without fixed count limits."""

    df, _, _ = _read_curated_data(spark, path)
    print("Incremental input validation PASSED.")
    return df


# ------------------------------------------------------------
# Full-load validation
# ------------------------------------------------------------

def validate_target_table(cursor):
    """Require an empty target for the original one-time full load."""

    cursor.execute("""
        SELECT COUNT(*)
        FROM public.air_quality_readings
    """)
    existing_count = cursor.fetchone()[0]

    if existing_count != 0:
        raise RuntimeError(
            f"Full load requires an empty source table; "
            f"found {existing_count:,} existing rows."
        )

    cursor.execute("""
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'air_quality_readings'
          AND column_name = 'timestamp'
    """)
    result = cursor.fetchone()

    if result is None:
        raise RuntimeError("Target table has no timestamp column.")

    if result[0] != "timestamp without time zone":
        raise TypeError(
            "Expected timestamp without time zone, "
            f"but found {result[0]}."
        )

    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'air_quality_readings'
    """)
    target_columns = {row[0] for row in cursor.fetchall()}
    missing_columns = sorted(set(COLUMNS) - target_columns)

    if missing_columns:
        raise RuntimeError(
            f"Neon table is missing required columns: {missing_columns}"
        )

    print("Full-load target validation PASSED.")


def validate_loaded_data(cursor):
    """Validate the original full load before committing."""

    cursor.execute("""
        SELECT
            COUNT(*),
            MIN("timestamp"),
            MAX("timestamp"),
            COUNT(DISTINCT (station_id, "timestamp")),
            COUNT(*) FILTER (
                WHERE date IS DISTINCT FROM "timestamp"::date
                   OR hour IS DISTINCT FROM
                      EXTRACT(HOUR FROM "timestamp")::INTEGER
            )
        FROM public.air_quality_readings
    """)

    total, earliest, latest, unique_keys, mismatches = cursor.fetchone()

    problems = []

    if total != EXPECTED_RECORDS:
        problems.append(f"Expected {EXPECTED_RECORDS:,} rows; found {total:,}.")

    if earliest != EXPECTED_MIN_TIMESTAMP:
        problems.append(f"Unexpected earliest timestamp: {earliest}")

    if latest != EXPECTED_MAX_TIMESTAMP:
        problems.append(f"Unexpected latest timestamp: {latest}")

    if unique_keys != EXPECTED_RECORDS:
        problems.append(f"Unexpected unique-key count: {unique_keys:,}")

    if mismatches:
        problems.append(f"Found {mismatches:,} date/hour mismatches.")

    if problems:
        raise RuntimeError(
            "Full-load validation failed:\n- " + "\n- ".join(problems)
        )

    print("Full-load post-insert validation PASSED.")


# ------------------------------------------------------------
# Original one-time full load
# ------------------------------------------------------------

def load_to_neon(curated_df, database_url):
    """Insert the original full dataset into an empty Neon table."""

    print("\nStarting one-time full load...")
    inserted_count = 0
    batch = []

    with psycopg2.connect(
        database_url,
        connect_timeout=30,
        application_name="air_quality_full_loader",
    ) as connection:
        with connection.cursor() as cursor:
            validate_target_table(cursor)

            for row in curated_df.toLocalIterator():
                record = tuple(row[column] for column in COLUMNS)
                timestamp_value = record[COLUMNS.index("timestamp")]

                if not isinstance(timestamp_value, datetime):
                    raise TypeError(
                        f"Invalid timestamp: {timestamp_value!r}"
                    )

                if timestamp_value.tzinfo is not None:
                    raise ValueError(
                        "Expected timezone-naive timestamps."
                    )

                batch.append(record)

                if len(batch) >= BATCH_SIZE:
                    execute_values(
                        cursor, BULK_INSERT_SQL, batch,
                        page_size=BATCH_SIZE,
                    )
                    inserted_count += len(batch)
                    print(f"Inserted {inserted_count:,} rows...")
                    batch.clear()

            if batch:
                execute_values(
                    cursor, BULK_INSERT_SQL, batch,
                    page_size=BATCH_SIZE,
                )
                inserted_count += len(batch)

            if inserted_count != EXPECTED_RECORDS:
                raise RuntimeError(
                    f"Attempted {inserted_count:,} inserts; "
                    f"expected {EXPECTED_RECORDS:,}."
                )

            validate_loaded_data(cursor)

    print(f"Full load committed: {inserted_count:,} rows.")


# ------------------------------------------------------------
# Incremental source-table loading
# ------------------------------------------------------------

INCREMENTAL_INSERT_SQL = """
    INSERT INTO public.air_quality_readings (
        city, state, station_id, station_name, "timestamp", date,
        day, hour, pm25, pm10, temperature, pressure,
        relative_humidity, wind_speed, wind_direction, rainfall,
        total_rainfall, solar_radiation, co, no, no2, nox, ozone,
        so2, benzene, toluene, ethylbenzene, mp_xylene, nh3,
        pm25_category, year, month
    )
    VALUES %s
    ON CONFLICT (station_id, "timestamp") DO NOTHING
"""


def load_incremental_to_neon(curated_df, cursor):
    """Insert missing source readings using the caller's transaction."""

    cursor.execute("""
        CREATE TEMP TABLE air_quality_batch_keys (
            station_id TEXT NOT NULL,
            reading_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            PRIMARY KEY (station_id, reading_timestamp)
        ) ON COMMIT DROP
    """)

    inserted_count = 0
    batch = []

    for row in curated_df.toLocalIterator():
        record = tuple(row[column] for column in COLUMNS)
        timestamp_value = record[COLUMNS.index("timestamp")]

        if not isinstance(timestamp_value, datetime):
            raise TypeError(f"Invalid timestamp: {timestamp_value!r}")

        if timestamp_value.tzinfo is not None:
            raise ValueError("Expected timezone-naive timestamps.")

        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            key_rows = [
                (row[COLUMNS.index("station_id")],
                 row[COLUMNS.index("timestamp")])
                for row in batch
            ]
            execute_values(
                cursor,
                "INSERT INTO air_quality_batch_keys VALUES %s "
                "ON CONFLICT DO NOTHING",
                key_rows,
                page_size=BATCH_SIZE,
            )
            execute_values(
                cursor,
                INCREMENTAL_INSERT_SQL,
                batch,
                page_size=BATCH_SIZE,
            )
            inserted_count += cursor.rowcount
            batch.clear()

    if batch:
        key_rows = [
            (row[COLUMNS.index("station_id")],
             row[COLUMNS.index("timestamp")])
            for row in batch
        ]
        execute_values(
            cursor,
            "INSERT INTO air_quality_batch_keys VALUES %s "
            "ON CONFLICT DO NOTHING",
            key_rows,
            page_size=BATCH_SIZE,
        )
        execute_values(
            cursor,
            INCREMENTAL_INSERT_SQL,
            batch,
            page_size=BATCH_SIZE,
        )
        inserted_count += cursor.rowcount

    print(f"Source readings inserted: {inserted_count:,}")
    return inserted_count


# ------------------------------------------------------------
# Star-schema dimension loaders
# Each function uses the caller's transaction.
# ------------------------------------------------------------

def load_dim_date(cursor):
    """Insert dates that are missing from dim_date."""

    cursor.execute("""
        INSERT INTO public.dim_date (
            date_key, full_date, day, month, year,
            weekday_name, is_weekend
        )
        SELECT DISTINCT
            TO_CHAR(r."timestamp"::date, 'YYYYMMDD')::INTEGER,
            r."timestamp"::date,
            EXTRACT(DAY FROM r."timestamp")::INTEGER,
            EXTRACT(MONTH FROM r."timestamp")::INTEGER,
            EXTRACT(YEAR FROM r."timestamp")::INTEGER,
            TRIM(TO_CHAR(r."timestamp", 'Day')),
            EXTRACT(ISODOW FROM r."timestamp") IN (6, 7)
        FROM public.air_quality_readings r
        JOIN air_quality_batch_keys b
          ON b.station_id = r.station_id
         AND b.reading_timestamp = r."timestamp"
        WHERE r."timestamp" IS NOT NULL
        ON CONFLICT DO NOTHING
    """)

    print(f"dim_date inserted: {cursor.rowcount:,}")


def load_dim_time(cursor):
    """Ensure all 24 hourly records exist."""

    cursor.execute("""
        INSERT INTO public.dim_time (time_key, hour, hour_label)
        SELECT
            h,
            h,
            LPAD(h::TEXT, 2, '0') || ':00'
        FROM GENERATE_SERIES(0, 23) AS hours(h)
        ON CONFLICT DO NOTHING
    """)

    print(f"dim_time inserted: {cursor.rowcount:,}")


def load_dim_station(cursor):
    """Insert stations and refresh metadata."""

    cursor.execute("""
        INSERT INTO public.dim_station (
            station_id, station_name, city, state
        )
        SELECT DISTINCT
            r.station_id, r.station_name, r.city, r.state
        FROM public.air_quality_readings r
        JOIN air_quality_batch_keys b
          ON b.station_id = r.station_id
         AND b.reading_timestamp = r."timestamp"
        WHERE r.station_id IS NOT NULL
        ON CONFLICT (station_id) DO UPDATE SET
            station_name = EXCLUDED.station_name,
            city = EXCLUDED.city,
            state = EXCLUDED.state
    """)

    print(f"dim_station inserted/updated: {cursor.rowcount:,}")


# ------------------------------------------------------------
# Star-schema fact loader
# ------------------------------------------------------------

def load_fact_air_quality(cursor):
    """Insert facts that do not already exist."""

    cursor.execute("""
        INSERT INTO public.fact_air_quality (
            station_key, date_key, time_key, reading_timestamp,
            pm25, pm10, temperature, pressure, relative_humidity,
            wind_speed, wind_direction, rainfall, total_rainfall,
            solar_radiation, co, no, no2, nox, ozone, so2,
            benzene, toluene, ethylbenzene, mp_xylene, nh3,
            pm25_category
        )
        SELECT
            s.station_key,
            d.date_key,
            t.time_key,
            r."timestamp",
            r.pm25,
            r.pm10,
            r.temperature,
            r.pressure,
            r.relative_humidity,
            r.wind_speed,
            r.wind_direction,
            r.rainfall,
            r.total_rainfall,
            r.solar_radiation,
            r.co,
            r.no,
            r.no2,
            r.nox,
            r.ozone,
            r.so2,
            r.benzene,
            r.toluene,
            r.ethylbenzene,
            r.mp_xylene,
            r.nh3,
            r.pm25_category
        FROM public.air_quality_readings r
        JOIN air_quality_batch_keys b
          ON b.station_id = r.station_id
         AND b.reading_timestamp = r."timestamp"
        JOIN public.dim_station s
          ON s.station_id = r.station_id
        JOIN public.dim_date d
          ON d.full_date = r."timestamp"::date
        JOIN public.dim_time t
          ON t.hour = EXTRACT(HOUR FROM r."timestamp")::INTEGER
        WHERE r."timestamp" IS NOT NULL
        ON CONFLICT (station_key, reading_timestamp) DO NOTHING
    """)

    print(f"fact_air_quality inserted: {cursor.rowcount:,}")


# ------------------------------------------------------------
# Star-schema verification
# ------------------------------------------------------------

def verify_star_schema(cursor):
    """Verify source/fact counts and dimension references for this batch."""

    cursor.execute("""
        SELECT
            (SELECT COUNT(*) FROM air_quality_batch_keys),
            (
                SELECT COUNT(*)
                FROM air_quality_batch_keys b
                JOIN public.air_quality_readings r
                  ON r.station_id = b.station_id
                 AND r."timestamp" = b.reading_timestamp
            ),
            (
                SELECT COUNT(*)
                FROM air_quality_batch_keys b
                JOIN public.air_quality_readings r
                  ON r.station_id = b.station_id
                 AND r."timestamp" = b.reading_timestamp
                JOIN public.fact_air_quality f
                  ON f.reading_timestamp = r."timestamp"
                 AND f.station_key = (
                     SELECT s.station_key
                     FROM public.dim_station s
                     WHERE s.station_id = r.station_id
                 )
            ),
            (
                SELECT COUNT(*)
                FROM air_quality_batch_keys b
                JOIN public.air_quality_readings r
                  ON r.station_id = b.station_id
                 AND r."timestamp" = b.reading_timestamp
                LEFT JOIN public.dim_station s
                  ON s.station_id = r.station_id
                LEFT JOIN public.dim_date d
                  ON d.full_date = r."timestamp"::date
                LEFT JOIN public.dim_time t
                  ON t.hour = EXTRACT(HOUR FROM r."timestamp")::INTEGER
                WHERE s.station_key IS NULL
                   OR d.date_key IS NULL
                   OR t.time_key IS NULL
            )
    """)

    (batch_count, source_count, fact_count,
     missing_source_dimensions) = cursor.fetchone()

    print("\nBatch star-schema verification:")
    print(f"Batch keys:      {batch_count:,}")
    print(f"Source readings: {source_count:,}")
    print(f"Fact readings:   {fact_count:,}")
    print(f"Source rows missing dimensions: {missing_source_dimensions:,}")

    if source_count != batch_count or fact_count != batch_count:
        raise RuntimeError(
            "Batch/source/fact row counts differ: "
            f"{batch_count:,}/{source_count:,}/{fact_count:,}."
        )

    if missing_source_dimensions != 0:
        raise RuntimeError("Star-schema dimension-reference checks failed.")

    print("Star-schema verification PASSED.")


# ------------------------------------------------------------
# Recurring incremental entry point
# ------------------------------------------------------------

def run_incremental_load():
    """
    Increment source readings and synchronize the star schema.

    All Neon changes use one transaction. If loading or verification
    fails, the database changes for this run are rolled back.
    """

    pending_batch = get_pending_batch()
    if pending_batch is None:
        print("No pending transformed batch; Neon load skipped.")
        return

    database_url = get_database_url()
    spark = None

    try:
        spark = create_spark_session()
        curated_df = read_incremental_curated_data(
            spark,
            pending_batch["path"],
        )

        with psycopg2.connect(
            database_url,
            connect_timeout=30,
            application_name="air_quality_incremental_pipeline",
        ) as connection:
            with connection.cursor() as cursor:
                inserted_count = load_incremental_to_neon(
                    curated_df,
                    cursor,
                )

                print(
                    f"New source readings inserted: {inserted_count:,}"
                )

                load_dim_date(cursor)
                load_dim_time(cursor)
                load_dim_station(cursor)
                load_fact_air_quality(cursor)

                verify_star_schema(cursor)

        complete_pending_batch(pending_batch["id"])
        print("Incremental source and star-schema load committed.")

    finally:
        if spark is not None:
            spark.stop()


# ------------------------------------------------------------
# One-time full-load entry point
# ------------------------------------------------------------

def main():
    """Run the guarded, one-time full source-table load."""

    database_url = get_database_url()
    spark = None

    try:
        spark = create_spark_session()
        curated_df = read_and_validate_curated_data(spark)

        print("\nFull-load input validation PASSED.")
        confirmation = input(
            f"Type LOAD to insert {EXPECTED_RECORDS:,} records into Neon: "
        ).strip()

        if confirmation != "LOAD":
            print("Load cancelled. Neon was not modified.")
            return

        load_to_neon(curated_df, database_url)

        # The full source load and initial star-schema sync are
        # separate transactions. If this sync fails, source data
        # remains loaded and the incremental path can be retried.
        with psycopg2.connect(
            database_url,
            connect_timeout=30,
            application_name="air_quality_initial_star_schema",
        ) as connection:
            with connection.cursor() as cursor:
                load_dim_date(cursor)
                load_dim_time(cursor)
                load_dim_station(cursor)
                load_fact_air_quality(cursor)
                verify_star_schema(cursor)

        print("Initial source load and star-schema sync completed.")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
