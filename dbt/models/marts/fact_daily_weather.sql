{{ config(materialized='table') }}

SELECT
    station_id,
    location,
    observation_date,
    COUNT(*) as observation_count,
    ROUND(AVG(temperature_degC), 2) as avg_temperature_degC,
    ROUND(MIN(temperature_degC), 2) as min_temperature_degC,
    ROUND(MAX(temperature_degC), 2) as max_temperature_degC,
    ROUND(AVG(humidity_pct), 2) as avg_humidity_pct,
    ROUND(AVG(wind_speed_mps), 2) as avg_wind_speed_mps,
    ROUND(MAX(wind_speed_mps), 2) as max_wind_speed_mps,
    ROUND(AVG(pressure_mb), 2) as avg_pressure_mb
FROM {{ ref('fact_observations') }}
GROUP BY
    station_id,
    location,
    observation_date
