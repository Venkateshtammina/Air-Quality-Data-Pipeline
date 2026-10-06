{{ config(
    materialized='incremental',
    unique_key=['station_id', 'timestamp']
) }}

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

FROM {{ ref('stg_air_quality_readings') }}

{% if is_incremental() %}

WHERE timestamp > (
    SELECT MAX(timestamp)
    FROM {{ this }}
)

{% endif %}