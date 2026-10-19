import json
import hashlib

import boto3

from pyspark.sql import SparkSession
from data_quality import validate_curated_data
from pyspark.sql.functions import (
    col,
    explode,
    to_timestamp_ntz,
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

# Tracks which exact raw S3 objects have already been transformed. ETags
# let a later overwrite of a page file be picked up on the next run.
TRANSFORM_MANIFEST_KEY = "checkpoints/air_quality_transform_manifest.json"
BATCH_PREFIX = f"s3a://{BUCKET}/curated/air_quality_batches/"


# ---------------------------------------------------------
# Discover raw ingestion-date paths
# ---------------------------------------------------------

def discover_raw_paths():
    """
    Find new or changed raw S3 files and return their Spark paths,
    together with the updated processed-object manifest.
    """

    print("=" * 60)
    print("DISCOVERING RAW AIR QUALITY DATA")
    print("=" * 60)

    s3 = boto3.client(
        "s3",
        region_name="eu-north-1"
    )

    paginator = s3.get_paginator("list_objects_v2")

    try:
        response = s3.get_object(
            Bucket=BUCKET,
            Key=TRANSFORM_MANIFEST_KEY,
        )
        manifest = json.loads(
            response["Body"].read().decode("utf-8")
        )
        manifest_exists = True
    except s3.exceptions.NoSuchKey:
        manifest = {"objects": {}, "pending_batch": None}
        manifest_exists = False

    # Migrate the original flat key-to-ETag manifest format.
    if "objects" not in manifest:
        manifest = {"objects": manifest, "pending_batch": None}

    processed_objects = manifest["objects"]
    if manifest.get("pending_batch"):
        print("A transformed batch is awaiting database completion.")
        return [], manifest, False

    changed_objects = {}

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=RAW_PREFIX
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.startswith(f"{RAW_PREFIX}ingestion_date="):
                continue

            if not key.endswith(".json"):
                continue

            etag = obj["ETag"].strip('"')
            if processed_objects.get(key) != etag:
                changed_objects[key] = etag

    if not changed_objects:
        print("No new or changed raw files; curated data is up to date.")
        return [], manifest, False

    raw_paths = [f"s3a://{BUCKET}/{key}" for key in sorted(changed_objects)]
    batch_fingerprint = "\n".join(
        f"{key}:{changed_objects[key]}" for key in sorted(changed_objects)
    )
    batch_id = hashlib.sha256(batch_fingerprint.encode("utf-8")).hexdigest()[:20]
    updated_manifest = {
        "objects": {**processed_objects, **changed_objects},
        "pending_batch": {
            "id": batch_id,
            "path": f"{BATCH_PREFIX}batch_id={batch_id}/",
        },
    }

    print(
        f"Discovered {len(raw_paths)} new or changed raw file(s):"
    )

    for path in raw_paths:
        print(f"  {path}")

    print()

    return raw_paths, updated_manifest, not manifest_exists


def save_transform_manifest(processed_objects):
    """Persist the manifest after the curated write has succeeded."""
    boto3.client("s3", region_name="eu-north-1").put_object(
        Bucket=BUCKET,
        Key=TRANSFORM_MANIFEST_KEY,
        Body=json.dumps(processed_objects, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


raw_paths, updated_manifest, bootstrap_run = discover_raw_paths()

if not raw_paths:
    spark.stop()
    raise SystemExit(0)


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
    to_timestamp_ntz(col("Timestamp"))
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

# On the first run only, remove keys already in curated Parquet before
# appending. Still send the full incoming batch to Neon so an existing
# warehouse can be backfilled idempotently. Recurring runs do not scan the
# historical curated dataset.
new_curated_df = curated_df
if bootstrap_run:
    curated_objects = boto3.client(
        "s3",
        region_name="eu-north-1",
    ).list_objects_v2(
        Bucket=BUCKET,
        Prefix="curated/air_quality/",
        MaxKeys=1,
    ).get("Contents", [])

    if curated_objects:
        existing_keys = (
            spark.read.parquet(CURATED_PATH)
            .select("station_id", "timestamp")
            .dropDuplicates(["station_id", "timestamp"])
        )
        new_curated_df = curated_df.join(
            existing_keys,
            on=["station_id", "timestamp"],
            how="left_anti",
        )

new_count = new_curated_df.count()
print(f"New curated records to append: {new_count}")

if curated_df.limit(1).count() == 0:
    save_transform_manifest({
        "objects": updated_manifest["objects"],
        "pending_batch": None,
    })
    print("No new business keys; downstream batch tasks can skip.")
    spark.stop()
    raise SystemExit(0)

batch_path = updated_manifest["pending_batch"]["path"]

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
    .parquet(batch_path)
)

if new_count:
    (
        new_curated_df
        .write
        .mode("append")
        .partitionBy("year", "month")
        .parquet(CURATED_PATH)
    )

print(
    "Curated data written successfully to:",
    CURATED_PATH
)

save_transform_manifest(updated_manifest)
print(f"Pending curated batch written to: {batch_path}")


# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()
