{{ config(materialized='table') }}

SELECT
    station_id,
    station_name,
    city,
    state,
    date,
    hour,

    ROUND(AVG(pm25)::numeric, 2) AS avg_pm25,
    ROUND(MAX(pm25)::numeric, 2) AS max_pm25,

    ROUND(AVG(pm10)::numeric, 2) AS avg_pm10,

    ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,

    ROUND(AVG(relative_humidity)::numeric, 2) AS avg_relative_humidity,

    ROUND(MAX(ozone)::numeric, 2) AS max_ozone,

    COUNT(*) AS reading_count

FROM {{ ref('stg_air_quality_readings') }}

GROUP BY
    station_id,
    station_name,
    city,
    state,
    date,
    hour