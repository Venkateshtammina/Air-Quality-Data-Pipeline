SELECT
    station_id,
    date
FROM {{ ref('daily_air_quality') }}
GROUP BY
    station_id,
    date
HAVING COUNT(*) > 1