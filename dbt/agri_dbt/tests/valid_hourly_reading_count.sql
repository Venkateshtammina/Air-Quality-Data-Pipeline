SELECT
    station_id,
    date,
    hour,
    reading_count
FROM {{ ref('hourly_air_quality') }}
WHERE reading_count < 1