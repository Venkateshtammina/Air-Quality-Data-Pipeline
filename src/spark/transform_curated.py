import boto3

from pyspark.sql import SparkSession
from data_quality import validate_curated_data
from pyspark.sql.functions import (
    col,
    explode,
    to_timestamp,
    to_date,
    year,
    month,
    dayofmonth,
    hour,
    when,
)


# ---------------------------------------------------------
# Spark configuration
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Transform Air Quality to Curated")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "software.amazon.awssdk:bundle:2.29.52"
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
        "spark.sql.session.timeZone",
        "Asia/Kolkata"
    )
    .config(
        "spark.hadoop.fs.s3a.fast.upload",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.fast.upload.buffer",
        "array"
    )
    .config(
        "spark.hadoop.fs.s3a.fast.upload.active.blocks",
        "1"
    )
    .getOrCreate()
)


# ---------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------

BUCKET = "agri-data-engineering"

RAW_PREFIX = "raw/air_quality/"

CURATED_PATH = (
    f"s3a://{BUCKET}/"
    "curated/air_quality/"
)


# ---------------------------------------------------------
# Discover raw ingestion-date paths
# ---------------------------------------------------------

def discover_raw_paths():
    """
    Discover all ingestion-date partitions under the raw
    air-quality prefix and return Spark-readable paths.
    """

    print("=" * 60)
    print("DISCOVERING RAW AIR QUALITY DATA")
    print("=" * 60)

    s3 = boto3.client(
        "s3",
        region_name="eu-north-1"
    )

    paginator = s3.get_paginator("list_objects_v2")

    ingestion_dates = set()

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=RAW_PREFIX
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.startswith(
                f"{RAW_PREFIX}ingestion_date="
            ):
                continue

            relative_key = key[len(RAW_PREFIX):]

            date_partition = relative_key.split("/", 1)[0]

            if date_partition.startswith(
                "ingestion_date="
            ):
                ingestion_dates.add(date_partition)

    if not ingestion_dates:
        raise RuntimeError(
            "No raw air-quality ingestion-date partitions "
            "were found in S3."
        )

    raw_paths = []

    for ingestion_date in sorted(ingestion_dates):
        path = (
            f"s3a://{BUCKET}/"
            f"{RAW_PREFIX}"
            f"{ingestion_date}/page_*.json"
        )

        raw_paths.append(path)

    print(
        f"Discovered {len(raw_paths)} "
        "ingestion-date partition(s):"
    )

    for path in raw_paths:
        print(f"  {path}")

    print()

    return raw_paths


raw_paths = discover_raw_paths()


# ---------------------------------------------------------
# Read raw JSON
# ---------------------------------------------------------

print("=" * 60)
print("READING RAW AIR QUALITY DATA")
print("=" * 60)

raw_df = (
    spark.read
    .option("multiLine", True)
    .json(raw_paths)
)

records_df = raw_df.select(
    explode("result.records").alias("record")
).select(
    "record.*"
)

print("Raw record count:", records_df.count())


# ---------------------------------------------------------
# Cast measurement columns
# ---------------------------------------------------------

double_columns = [
    "AT (degC)",
    "BP (mmHg)",
    "Benzene (ug/m3)",
    "CO (mg/m3)",
    "Eth-Benzene (ug/m3)",
    "MP-Xylene (ug/m3)",
    "NH3 (ug/m3)",
    "NO (ug/m3)",
    "NO2 (ug/m3)",
    "NOx (ppb)",
    "Ozone (ug/m3)",
    "PM10 (ug/m3)",
    "PM2.5 (ug/m3)",
    "RF (mm)",
    "RH (%)",
    "SO2 (ug/m3)",
    "SR (W/mt2)",
    "TOT-RF (mm)",
    "Toluene (ug/m3)",
    "WD (deg)",
    "WS (m/s)",
]

for column_name in double_columns:
    records_df = records_df.withColumn(
        column_name,
        col(f"`{column_name}`").cast("double")
    )


records_df = records_df.withColumn(
    "Timestamp",
    to_timestamp(col("Timestamp"))
)


# ---------------------------------------------------------
# Remove completely empty columns
# ---------------------------------------------------------

records_df = records_df.drop(
    "O Xylene (ug/m3)",
    "VWS (m/s)",
    "Xylene (ug/m3)"
)


# ---------------------------------------------------------
# Rename columns to analytics-friendly names
# ---------------------------------------------------------

rename_map = {
    "AT (degC)": "temperature",
    "BP (mmHg)": "pressure",
    "Benzene (ug/m3)": "benzene",
    "CO (mg/m3)": "co",
    "Eth-Benzene (ug/m3)": "ethylbenzene",
    "MP-Xylene (ug/m3)": "mp_xylene",
    "NH3 (ug/m3)": "nh3",
    "NO (ug/m3)": "no",
    "NO2 (ug/m3)": "no2",
    "NOx (ppb)": "nox",
    "Ozone (ug/m3)": "ozone",
    "PM10 (ug/m3)": "pm10",
    "PM2.5 (ug/m3)": "pm25",
    "RF (mm)": "rainfall",
    "RH (%)": "relative_humidity",
    "SO2 (ug/m3)": "so2",
    "SR (W/mt2)": "solar_radiation",
    "Station ID": "station_id",
    "Station Name": "station_name",
    "State": "state",
    "City": "city",
    "TOT-RF (mm)": "total_rainfall",
    "Timestamp": "timestamp",
    "Toluene (ug/m3)": "toluene",
    "WD (deg)": "wind_direction",
    "WS (m/s)": "wind_speed",
}

for old_name, new_name in rename_map.items():
    records_df = records_df.withColumnRenamed(
        old_name,
        new_name
    )


# ---------------------------------------------------------
# Remove invalid timestamps
# ---------------------------------------------------------

records_df = records_df.filter(
    col("timestamp").isNotNull()
)


# ---------------------------------------------------------
# Deduplicate business key
# ---------------------------------------------------------

records_df = records_df.dropDuplicates(
    ["station_id", "timestamp"]
)


# ---------------------------------------------------------
# Add date/time attributes
# ---------------------------------------------------------

curated_df = (
    records_df
    .withColumn(
        "date",
        to_date(col("timestamp"))
    )
    .withColumn(
        "year",
        year(col("timestamp"))
    )
    .withColumn(
        "month",
        month(col("timestamp"))
    )
    .withColumn(
        "day",
        dayofmonth(col("timestamp"))
    )
    .withColumn(
        "hour",
        hour(col("timestamp"))
    )
)


# ---------------------------------------------------------
# Add PM2.5 category
# ---------------------------------------------------------

curated_df = curated_df.withColumn(
    "pm25_category",
    when(
        col("pm25").isNull(),
        "Unknown"
    )
    .when(
        col("pm25") <= 30,
        "Good"
    )
    .when(
        col("pm25") <= 60,
        "Satisfactory"
    )
    .when(
        col("pm25") <= 90,
        "Moderate"
    )
    .when(
        col("pm25") <= 120,
        "Poor"
    )
    .when(
        col("pm25") <= 250,
        "Very Poor"
    )
    .otherwise(
        "Severe"
    )
)


# ---------------------------------------------------------
# Select final curated columns
# ---------------------------------------------------------

curated_columns = [
    "city",
    "state",
    "station_id",
    "station_name",
    "timestamp",
    "date",
    "year",
    "month",
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
]

curated_columns = list(
    dict.fromkeys(curated_columns)
)

curated_df = curated_df.select(
    *curated_columns
)


# ---------------------------------------------------------
# Final validation
# ---------------------------------------------------------

validate_curated_data(curated_df)

print()
print("=" * 60)
print("CURATED DATA VALIDATION")
print("=" * 60)

record_count = curated_df.count()

print("Curated records:", record_count)
print("Curated columns:", len(curated_df.columns))

print("\nCurated schema:")
curated_df.printSchema()

print("\nSample curated records:")
curated_df.show(10, truncate=False)


# ---------------------------------------------------------
# Write curated Parquet
# ---------------------------------------------------------

print()
print("=" * 60)
print("WRITING CURATED DATA TO S3")
print("=" * 60)

(
    curated_df
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(CURATED_PATH)
)

print(
    "Curated data written successfully to:",
    CURATED_PATH
)


# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()