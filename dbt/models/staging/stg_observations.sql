{{ config(materialized='view') }}

SELECT
    station as station_id,
    location,
    observation_time as observation_timestamp,
    ROUND((temperature_f - 32) * (5/9), 2) as temperature_degC,
    temperature_f,
    humidity_pct,
    ROUND(wind_speed_mph * 0.44704, 2) as wind_speed_mps,
    wind_speed_mph,
    pressure_mb,
    condition,
    CURRENT_TIMESTAMP as ingestion_timestamp
FROM observations
WHERE observation_time IS NOT NULL
