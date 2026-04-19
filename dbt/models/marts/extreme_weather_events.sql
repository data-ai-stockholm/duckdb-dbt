{{ config(materialized='table') }}

WITH station_stats AS (
    SELECT
        station_id,
        AVG(temperature_degC) as avg_temp,
        STDDEV(temperature_degC) as stddev_temp
    FROM {{ ref('fact_observations') }}
    GROUP BY station_id
)

SELECT
    o.observation_id,
    o.observation_timestamp,
    o.station_id,
    o.temperature_degC,
    s.avg_temp,
    s.stddev_temp,
    ABS(o.temperature_degC - s.avg_temp) / NULLIF(s.stddev_temp, 0) as temp_z_score,
    CASE
        WHEN ABS(o.temperature_degC - s.avg_temp) / NULLIF(s.stddev_temp, 0) > 2 THEN 'Extreme'
        WHEN ABS(o.temperature_degC - s.avg_temp) / NULLIF(s.stddev_temp, 0) > 1 THEN 'Unusual'
        ELSE 'Normal'
    END as temperature_category
FROM {{ ref('fact_observations') }} o
JOIN station_stats s ON o.station_id = s.station_id
WHERE s.stddev_temp IS NOT NULL
    AND ABS(o.temperature_degC - s.avg_temp) / NULLIF(s.stddev_temp, 0) > 1