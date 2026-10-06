from pyspark.sql.functions import (
    col,
    sum as spark_sum,
)


def validate_curated_data(df):
    """
    Run all critical data quality checks on curated data.
    """

    print()
    print("=" * 60)
    print("DATA QUALITY VALIDATION")
    print("=" * 60)

    # ---------------------------------------------------------
    # Basic validation checks
    # ---------------------------------------------------------

    validation_result = df.select(
        spark_sum(
            col("station_id").isNull().cast("int")
        ).alias("null_station_id"),

        spark_sum(
            col("timestamp").isNull().cast("int")
        ).alias("null_timestamp"),

        spark_sum(
            (
                col("pm25").isNotNull() &
                (col("pm25") < 0)
            ).cast("int")
        ).alias("invalid_pm25"),

        spark_sum(
            (
                col("wind_speed").isNotNull() &
                (col("wind_speed") < 0)
            ).cast("int")
        ).alias("invalid_wind_speed"),

        spark_sum(
            (
                col("relative_humidity").isNotNull() &
                (
                    (col("relative_humidity") < 0) |
                    (col("relative_humidity") > 100)
                )
            ).cast("int")
        ).alias("invalid_relative_humidity"),

        spark_sum(
            (
                col("wind_direction").isNotNull() &
                (
                    (col("wind_direction") < 0) |
                    (col("wind_direction") >= 360)
                )
            ).cast("int")
        ).alias("invalid_wind_direction"),
    ).collect()[0]

    record_count = df.count()

    null_station_id = validation_result["null_station_id"] or 0
    null_timestamp = validation_result["null_timestamp"] or 0
    invalid_pm25 = validation_result["invalid_pm25"] or 0
    invalid_wind_speed = validation_result["invalid_wind_speed"] or 0
    invalid_relative_humidity = (
        validation_result["invalid_relative_humidity"] or 0
    )
    invalid_wind_direction = (
        validation_result["invalid_wind_direction"] or 0
    )

    # ---------------------------------------------------------
    # Record count check
    # ---------------------------------------------------------

    if record_count == 0:
        raise ValueError(
            "Data quality check failed: dataset contains zero records."
        )

    # ---------------------------------------------------------
    # Required field checks
    # ---------------------------------------------------------

    if null_station_id > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{null_station_id} records have null station_id."
        )

    if null_timestamp > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{null_timestamp} records have null timestamp."
        )

    # ---------------------------------------------------------
    # Measurement range checks
    # ---------------------------------------------------------

    if invalid_pm25 > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{invalid_pm25} invalid PM2.5 values found."
        )

    if invalid_wind_speed > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{invalid_wind_speed} invalid wind speed values found."
        )

    if invalid_relative_humidity > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{invalid_relative_humidity} invalid relative humidity values found."
        )

    if invalid_wind_direction > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{invalid_wind_direction} invalid wind direction values found."
        )

    # ---------------------------------------------------------
    # Duplicate business-key check
    # ---------------------------------------------------------

    duplicate_groups = (
        df.groupBy(
            "station_id",
            "timestamp"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_groups > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{duplicate_groups} duplicate "
            f"station_id + timestamp groups found."
        )

    # ---------------------------------------------------------
    # Final result
    # ---------------------------------------------------------

    print(
        f"Data quality check passed: "
        f"record count = {record_count}"
    )

    print(
        "Data quality check passed: "
        "station_id and timestamp contain no null values."
    )

    print(
        "Data quality check passed: "
        "station_id + timestamp is unique."
    )

    print(
        "Data quality check passed: "
        "all measurement ranges are valid."
    )

    print()
    print("=" * 60)
    print("DATA QUALITY CHECKS PASSED")
    print("=" * 60)