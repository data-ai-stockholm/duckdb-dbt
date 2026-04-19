{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY observation_timestamp) as observation_id,
    observation_timestamp,
    station_id,
    location,
    temperature_degC,
    temperature_f,
    humidity_pct,
    wind_speed_mps,
    wind_speed_mph,
    pressure_mb,
    condition,
    DATE_TRUNC('day', observation_timestamp) as observation_date,
    EXTRACT(HOUR FROM observation_timestamp) as observation_hour,
    EXTRACT(DOW FROM observation_timestamp) as day_of_week
FROM {{ ref('stg_observations') }}