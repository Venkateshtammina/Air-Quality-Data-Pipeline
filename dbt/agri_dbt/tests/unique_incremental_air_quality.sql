SELECT
    station_id,
    timestamp
FROM {{ ref('air_quality_incremental') }}
GROUP BY
    station_id,
    timestamp
HAVING COUNT(*) > 1