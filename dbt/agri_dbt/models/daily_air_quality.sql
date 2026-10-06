SELECT
    station_id,
    station_name,
    city,
    state,
    date,

    ROUND(AVG(avg_pm25)::numeric, 2) AS avg_pm25,
    ROUND(MAX(max_pm25)::numeric, 2) AS max_pm25,

    ROUND(AVG(avg_pm10)::numeric, 2) AS avg_pm10,

    ROUND(AVG(avg_temperature)::numeric, 2) AS avg_temperature,

    ROUND(AVG(avg_relative_humidity)::numeric, 2) AS avg_relative_humidity,

    ROUND(MAX(max_ozone)::numeric, 2) AS max_ozone,

    SUM(reading_count) AS reading_count

FROM {{ ref('hourly_air_quality') }}

GROUP BY
    station_id,
    station_name,
    city,
    state,
    date