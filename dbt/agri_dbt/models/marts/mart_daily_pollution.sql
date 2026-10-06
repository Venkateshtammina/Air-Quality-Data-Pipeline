SELECT
    station_id,
    station_name,
    city,
    state,
    date,

    avg_pm25,
    max_pm25,
    avg_pm10,
    avg_temperature,
    avg_relative_humidity,
    max_ozone,
    reading_count,

    RANK() OVER (
        ORDER BY avg_pm25 DESC NULLS LAST
    ) AS pollution_rank

FROM {{ ref('daily_air_quality') }}