import duckdb
import os

from weather_api import fetch_all_zones, fetch_station_group, fetch_latest_observation_for_station

def read_all_zones(zones_db: duckdb.DuckDBPyConnection | None) -> duckdb.DuckDBPyConnection | None:
    if zones_db is None:
        print("Invalid database connection.")
        return None

    zones_db.execute(f"""
        CREATE OR REPLACE TABLE all_zones AS
        
        SELECT
            UNNEST(features)['id'] AS zone_id,
            UNNEST(features)['properties']['@id'] AS zone_url,
            UNNEST(features)['properties']['type'] AS zone_type,
            UNNEST(features)['properties']['name'] AS zone_name,
            UNNEST(features)['properties']['state'] AS state,
            UNNEST(features)['properties']['forecastOffice'] AS office_url
        FROM all_zones
    """)

    return zones_db

def write_all_zones(zones_db: duckdb.DuckDBPyConnection | None):
    if zones_db is None:
        print("Invalid database connection.")
        return

    if not os.path.exists('ingestion_data/zones/'):
        print("Directory 'ingestion_data/zones/' does not exist. Creating it now.")
        os.makedirs('ingestion_data/zones/')
        print("Created directory: ingestion_data/zones/")

    zones_db.execute(f"""
        COPY (FROM all_zones) TO 'ingestion_data/zones/zones.parquet' (FORMAT PARQUET)
    """)

    print(f"Saved zones data to ingestion_data/zones/zones.parquet")

def read_station_group(stations_db: duckdb.DuckDBPyConnection | None) -> duckdb.DuckDBPyConnection | None:
    if stations_db is None:
        print("Invalid stations_db connection.")
        return None

    try:
        stations_db.execute(
            f"""
            CREATE OR REPLACE TABLE station_groups AS
            SELECT
                UNNEST(features)['properties']['stationIdentifier'] AS station_id,
                UNNEST(features)['id'] AS station_url,
                CAST(UNNEST(features)['geometry']['coordinates'] AS VARCHAR[]) AS coordinates,
                UNNEST(features)['properties']['elevation']['value']::DECIMAL AS elevation_m,
                UNNEST(features)['properties']['name'] AS station_name,
                UNNEST(features)['properties']['timeZone'] AS time_zone,
                UNNEST(features)['properties']['forecast'] AS forecast_url,
                UNNEST(features)['properties']['county'] AS county,
                pagination.next AS next_page
            FROM station_groups
            """
        )
    except Exception as e:
        print(f"Error reading station groups: {e}")
        return None

    return stations_db

def write_station_group(stations_db: duckdb.DuckDBPyConnection | None) -> str | None:
    if stations_db is None:
        print("Invalid stations_db connection.")
        return
    
    next_page = stations_db.execute("""
            SELECT
                DISTINCT next_page
            FROM station_groups
        """).fetchone()[0]
    
    cursor = next_page.split("cursor=")[-1]

    if not os.path.exists('ingestion_data/stations/'):
        print("Directory 'ingestion_data/stations/' does not exist. Creating it now.")
        os.makedirs('ingestion_data/stations/')
        print("Created directory: ingestion_data/stations/")

    stations_db.execute(f"""
            COPY (FROM station_groups) TO 'ingestion_data/stations/station_group_{cursor}.parquet' (FORMAT PARQUET)
        """)
    
    print(f"Saved station groups data to ingestion_data/stations/station_groups.parquet")

    if next_page:
        return next_page

def read_latest_observation(observation_db: duckdb.DuckDBPyConnection | None) -> duckdb.DuckDBPyConnection | None:
    if observation_db is None:
        return None
    
    try:
        observation_db.execute(f"""
            CREATE OR REPLACE TABLE latest_observation AS
            SELECT
                CAST(properties.timestamp AS TIMESTAMPTZ) AS observation_timestamp,
                id AS observation_id,
                type AS observation_type,
                geometry.type AS geometry_type,
                CAST(json_extract(geometry, '$.coordinates') AS VARCHAR[]) AS coordinates,
                properties.elevation.value::DECIMAL AS elevation_m,
                properties.stationId AS station_id,
                properties.stationName AS station_name,
                properties.temperature.value::DECIMAL AS temperature_degC,
                properties.dewpoint.value::DECIMAL AS dewpoint_degC,
                properties.windDirection.value::DECIMAL AS wind_direction_deg,
                properties.windSpeed.value::DECIMAL AS wind_speed_kmh,
                properties.windGust.value::DECIMAL AS wind_gust_kmh,
                properties.barometricPressure.value::DECIMAL AS barometric_pressure_Pa,
                properties.seaLevelPressure.value::DECIMAL AS sea_level_pressure_Pa,
                properties.visibility.value::DECIMAL AS visibility_m,
                properties.maxTemperatureLast24Hours.value::DECIMAL AS max_temp_24h_degC,
                properties.minTemperatureLast24Hours.value::DECIMAL AS min_temp_24h_degC,
                properties.precipitationLast3Hours.value::DECIMAL AS precipitation_3h_mm,
                properties.relativeHumidity.value::DECIMAL AS relative_humidity_percent,
                properties.windChill.value::DECIMAL AS wind_chill_degC,
                properties.heatIndex.value::DECIMAL AS heat_index_degC
            FROM latest_observation
        """).df()
    except Exception as e:
        print(f"Error reading latest observation: {e}")
        return None
    
    return observation_db

def write_latest_observation(observation_db: duckdb.DuckDBPyConnection | None):
    if observation_db is None:
        return None
    
    station_id = observation_db.execute(f"""
        SELECT
            DISTINCT station_id
        FROM latest_observation
    """).fetchone()[0]
    
    fetch_date = observation_db.execute(f"""
            SELECT
                SPLIT(observation_timestamp::VARCHAR, ' ')[1] AS latest_time
            FROM latest_observation
        """).fetchone()[0]

    latest_timestamp = observation_db.execute(f"""
        SELECT
            SPLIT(observation_timestamp::VARCHAR, ' ')[2] AS latest_time
        FROM latest_observation
    """).fetchone()[0]

    if not os.path.exists(f"ingestion_data/observations/{station_id}/{fetch_date}"):
        os.makedirs(f"ingestion_data/observations/{station_id}/{fetch_date}")

    observation_db.execute(f"""
        COPY (
            SELECT
                *
            FROM latest_observation
        ) TO 'ingestion_data/observations/{station_id}/{fetch_date}/observation_{latest_timestamp}.parquet' (FORMAT PARQUET)
    """)

    print(f"Saved latest observation for station {station_id} at {latest_timestamp} to ingestion_data/observations/{station_id}/{fetch_date}/observation_{latest_timestamp}.parquet")

if __name__ == "__main__":
    # CODE FOR TESTING FUNCTIONS
    write_all_zones(read_all_zones(fetch_all_zones()))
    write_station_group(read_station_group(fetch_station_group()))
    write_latest_observation(read_latest_observation(fetch_latest_observation_for_station("014CE")))