SELECT
    station_id,
    date
FROM {{ ref('mart_daily_pollution') }}
GROUP BY
    station_id,
    date
HAVING COUNT(*) > 1