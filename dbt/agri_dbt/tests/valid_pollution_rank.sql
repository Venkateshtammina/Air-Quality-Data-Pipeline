SELECT
    station_id,
    date,
    avg_pm25,
    pollution_rank
FROM {{ ref('mart_daily_pollution') }}
WHERE avg_pm25 IS NOT NULL
  AND pollution_rank < 1