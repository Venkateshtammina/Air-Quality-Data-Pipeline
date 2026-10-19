from pyspark.sql import SparkSession

from data_quality import validate_curated_data
from batch_state import get_pending_batch


CURATED_PATH = "s3a://agri-data-engineering/curated/air_quality/"


spark = (
    SparkSession.builder
    .appName("Air Quality Data Quality Gate")
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
        "spark.driver.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .config(
        "spark.executor.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .getOrCreate()
)


try:
    print("=" * 60)
    print("AIR QUALITY DATA QUALITY GATE")
    print("=" * 60)

    pending_batch = get_pending_batch()
    if pending_batch is None:
        print("No pending transformed batch; quality gate has nothing to check.")
        raise SystemExit(0)

    batch_path = pending_batch["path"]
    print(f"Reading current curated batch from: {batch_path}")

    curated_df = spark.read.parquet(batch_path)

    row_count = curated_df.count()

    print(f"Curated records received: {row_count}")

    if row_count:
        validate_curated_data(curated_df)
    else:
        print("Batch contains no new business keys; skipping row checks.")

    print(f"Curated records validated: {row_count}")
    print("Quality status: PASS")
    print()
    print("=" * 60)
    print("DATA QUALITY GATE PASSED")
    print("=" * 60)

finally:
    spark.stop()
