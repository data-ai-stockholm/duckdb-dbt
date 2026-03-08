import duckdb

def fetch_all_zones() -> duckdb.DuckDBPyConnection | None:
    print("Fetching all zones from API...")
    conn = duckdb.connect()

    url = "https://api.weather.gov/zones"

    try:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE all_zones AS
            SELECT
                *
            FROM read_json_auto('{url}')
            """
        )
        return conn
    
    except Exception as e:
        print(f"Error fetching zones: {e}")
        return None
    
def fetch_station_group(limit: int = None, cursor: str = None) -> duckdb.DuckDBPyConnection | None:
    conn = duckdb.connect()

    if limit and not cursor:
        url = f"https://api.weather.gov/stations?limit={limit}"
    elif limit and cursor:
        url = f"https://api.weather.gov/stations?limit={limit}&cursor={cursor}"
    else:
        url = "https://api.weather.gov/stations"

    try:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE station_groups AS
            SELECT
                *
            FROM read_json_auto('{url}')
            """
        )
        return conn
    
    except Exception as e:
        print(f"Error fetching station groups: {e}")
        return None
    
def fetch_latest_observation_for_station(station_id: str) -> duckdb.DuckDBPyConnection | None:
    conn = duckdb.connect()
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"

    try:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE latest_observation AS
            SELECT
                *
            FROM read_json_auto('{url}')
            """
        )
        return conn

    except duckdb.HTTPException as e:
        print(f"Did NOT save observations for station: {station_id} due to HTTP Error")
        return None
    except duckdb.BinderException as e:
        print(f"Did NOT save observations for station: {station_id} due to Binder Error")
        return None
    except Exception as e:
        print(f"Error fetching latest observation for station {station_id}")
        return None

if __name__ == "__main__":
    # CODE FOR TESTING THE FUNCTIONS
    all_zones_conn = fetch_all_zones()
    if all_zones_conn:
        print("Successfully fetched all zones.")
        print(all_zones_conn.execute("SELECT COUNT(*) FROM all_zones").fetchone()[0])

    station_groups_conn = fetch_station_group()

    if station_groups_conn:
        print("Successfully fetched station groups.")
        print(station_groups_conn.execute("SELECT COUNT(*) FROM station_groups").fetchone()[0])

    # Example: Fetch latest observation for a specific station (replace with actual station ID)
    station_id = "017CE"  # Example station ID for Seattle-Tacoma International  Airport
    latest_observation_conn = fetch_latest_observation_for_station(station_id)

    if latest_observation_conn:
        print("Successfully fetched latest observation.")
        print(latest_observation_conn.execute("SELECT COUNT(*) FROM latest_observation").fetchone()[0])