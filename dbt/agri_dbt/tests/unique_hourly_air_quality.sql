SELECT
    station_id,
    date,
    hour
FROM {{ ref('hourly_air_quality') }}
GROUP BY
    station_id,
    date,
    hour
HAVING COUNT(*) > 1