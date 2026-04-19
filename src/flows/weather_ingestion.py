"""Prefect flow for weather data ingestion."""

from datetime import timedelta

from prefect import flow, task
from prefect.tasks import task_input_hash

from src.ingestion.fetch_observations import main as fetch_observations_main

# Import the actual functions instead of using subprocess
from src.ingestion.fetch_stations import main as fetch_stations_main
from src.ingestion.write_observations import main as load_observations_main


@task(
    name="fetch-weather-stations",
    description="Fetch weather station metadata from NWS API",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24),
)
def fetch_stations():
    """Fetch weather station data."""
    print("Fetching weather stations...")
    fetch_stations_main()
    return "Stations fetched successfully"


@task(
    name="fetch-weather-observations",
    description="Fetch weather observations from NWS API",
    retries=3,
    retry_delay_seconds=30,
)
def fetch_observations():
    """Fetch weather observation data."""
    print("Fetching weather observations...")
    fetch_observations_main()
    return "Observations fetched successfully"


@task(
    name="load-observations-to-iceberg",
    description="Load observations into Iceberg tables",
    retries=2,
    retry_delay_seconds=60,
)
def load_observations():
    """Load observations into Iceberg tables."""
    print("Loading observations to Iceberg...")
    load_observations_main()
    return "Observations loaded successfully"


@flow(
    name="weather-data-ingestion",
    description="Complete weather data ingestion pipeline",
    log_prints=True,
)
def weather_ingestion_flow():
    """
    Main flow for weather data ingestion.

    Steps:
    1. Fetch station metadata (cached for 24h)
    2. Fetch latest weather observations
    3. Load observations into Iceberg tables
    """
    print("Starting weather data ingestion pipeline...")

    try:
        # Fetch station metadata (cached)
        stations_result = fetch_stations()
        print(f"✓ {stations_result}")

        # Fetch observations
        observations_result = fetch_observations()
        print(f"✓ {observations_result}")

        # Load to Iceberg
        load_result = load_observations()
        print(f"✓ {load_result}")
        print("\n✅ Data loaded to Iceberg successfully")

        return {
            "status": "success",
            "stations": stations_result,
            "observations": observations_result,
            "load": load_result,
        }
    except Exception as e:
        print(f"\n❌ Error during ingestion: {str(e)}")
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    weather_ingestion_flow()
