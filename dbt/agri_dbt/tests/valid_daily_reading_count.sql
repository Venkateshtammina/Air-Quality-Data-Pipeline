SELECT
    station_id,
    date,
    reading_count
FROM {{ ref('daily_air_quality') }}
WHERE reading_count < 1