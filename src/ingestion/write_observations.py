"""Writes the observations data from Parquet files into DuckDB/Iceberg tables."""

from pathlib import Path

from src.ingestion.config import get_config
from src.ingestion.iceberg_manager import IcebergManager, get_duckdb_connection


def main():
    """Load observations data from Parquet files into DuckDB or Iceberg tables."""
    config = get_config()

    print("\n" + "=" * 70)
    print("Weather Data Pipeline - Loading Observations")
    print("=" * 70)
    print(f"Storage Backend: {config.storage_backend}")
    print(f"Table Format: {config.table_format}")
    print("=" * 70 + "\n")

    # Create DuckDB connection
    conn = get_duckdb_connection()

    # Initialize Iceberg manager
    iceberg = IcebergManager(conn)

    # Define the observations table schema
    observations_schema = """
        observation_timestamp TIMESTAMPTZ NOT NULL,
        observation_id VARCHAR NOT NULL,
        observation_type VARCHAR,
        geometry_type VARCHAR,
        geometry_coordinates VARCHAR[],
        elevation_m DOUBLE,
        station_id VARCHAR NOT NULL,
        station_name VARCHAR,
        temperature_degC DOUBLE,
        dewpoint_degC DOUBLE,
        wind_direction_deg DOUBLE,
        wind_speed_kmh DOUBLE,
        wind_gust_kmh DOUBLE,
        barometric_pressure_Pa DOUBLE,
        sea_level_pressure_Pa DOUBLE,
        visibility_m DOUBLE,
        max_temp_24h_degC DOUBLE,
        min_temp_24h_degC DOUBLE,
        precipitation_3h_mm DOUBLE,
        relative_humidity_percent DOUBLE,
        wind_chill_degC DOUBLE,
        heat_index_degC DOUBLE
    """

    # Get the parquet files path
    data_dir = config.data_dir
    parquet_pattern = f"{data_dir}/observations/observations_station_*.parquet"

    # Check if parquet files exist
    parquet_files = list(Path(data_dir).glob("observations/observations_station_*.parquet"))
    if not parquet_files:
        print(f"⚠ No parquet files found at: {parquet_pattern}")
        print("  Please run fetch_observations.py first")
        return

    print(f"Found {len(parquet_files)} observation files\n")

    # Create the observations table
    print("Creating observations table...")
    iceberg.create_table("observations", observations_schema, namespace="weather_data")

    # Define the data extraction query from Parquet files
    load_query = f"""
        SELECT
            CAST(observation.properties.timestamp AS TIMESTAMPTZ) AS observation_timestamp,
            json_extract_string(observation, '$.id') AS observation_id,
            json_extract_string(observation, '$.type') AS observation_type,
            json_extract_string(observation.geometry, '$.type') AS geometry_type,
            CAST(json_extract(observation.geometry, '$.coordinates') AS VARCHAR[]) AS geometry_coordinates,
            observation.properties.elevation.value::DOUBLE AS elevation_m,
            json_extract_string(observation.properties, '$.stationId') AS station_id,
            json_extract_string(observation.properties, '$.stationName') AS station_name,
            observation.properties.temperature.value::DOUBLE AS temperature_degC,
            observation.properties.dewpoint.value::DOUBLE AS dewpoint_degC,
            observation.properties.windDirection.value::DOUBLE AS wind_direction_deg,
            observation.properties.windSpeed.value::DOUBLE AS wind_speed_kmh,
            observation.properties.windGust.value::DOUBLE AS wind_gust_kmh,
            observation.properties.barometricPressure.value::DOUBLE AS barometric_pressure_Pa,
            observation.properties.seaLevelPressure.value::DOUBLE AS sea_level_pressure_Pa,
            observation.properties.visibility.value::DOUBLE AS visibility_m,
            observation.properties.maxTemperatureLast24Hours.value::DOUBLE AS max_temp_24h_degC,
            observation.properties.minTemperatureLast24Hours.value::DOUBLE AS min_temp_24h_degC,
            observation.properties.precipitationLast3Hours.value::DOUBLE AS precipitation_3h_mm,
            observation.properties.relativeHumidity.value::DOUBLE AS relative_humidity_percent,
            observation.properties.windChill.value::DOUBLE AS wind_chill_degC,
            observation.properties.heatIndex.value::DOUBLE AS heat_index_degC
        FROM read_parquet('{parquet_pattern}')
    """

    # Write data to the table
    print("Loading observation data...")
    iceberg.write_data(
        table_name="observations", query=load_query, namespace="weather_data", mode="overwrite"
    )

    # Get record count
    table_ref = iceberg.read_table("observations", namespace="weather_data")
    count_result = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()
    record_count = count_result[0] if count_result else 0

    print(f"\n✓ Successfully loaded {record_count:,} observations")

    # Show sample data
    print("\nSample data (first 5 records):")
    print("-" * 70)
    sample = conn.execute(f"""
        SELECT
            observation_timestamp,
            station_name,
            temperature_degC,
            wind_speed_kmh,
            relative_humidity_percent
        FROM {table_ref}
        ORDER BY observation_timestamp DESC
        LIMIT 5
    """).df()
    print(sample.to_string(index=False))

    # Show statistics
    print("\n" + "-" * 70)
    print("Data Statistics:")
    print("-" * 70)
    stats = conn.execute(f"""
        SELECT
            COUNT(DISTINCT station_id) as unique_stations,
            MIN(observation_timestamp) as earliest_observation,
            MAX(observation_timestamp) as latest_observation,
            AVG(temperature_degC) as avg_temp_degC,
            MIN(temperature_degC) as min_temp_degC,
            MAX(temperature_degC) as max_temp_degC
        FROM {table_ref}
    """).df()
    print(stats.to_string(index=False))

    conn.close()
    print("\n✓ Data loading complete!")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
