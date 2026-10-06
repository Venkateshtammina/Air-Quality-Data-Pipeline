SELECT
    station_id,
    timestamp
FROM {{ source('raw', 'air_quality_readings') }}
GROUP BY
    station_id,
    timestamp
HAVING COUNT(*) > 1