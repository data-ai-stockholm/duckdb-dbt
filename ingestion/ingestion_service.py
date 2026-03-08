from weather_api import fetch_all_zones, fetch_station_group, fetch_latest_observation_for_station
from ingestion import read_all_zones, write_all_zones, read_station_group, write_station_group, read_latest_observation, write_latest_observation
from datetime import datetime
import os
import duckdb
import argparse

parser = argparse.ArgumentParser(description="Ingestion service")
parser.add_argument("--fetch_station_group_limit", type=int, default=10, help="Number of stations to fetch per API call (for pagination)")
parser.add_argument("--cadence_seconds", type=int, default=60, help="Seconds between observation fetches")
_args = parser.parse_args()

fetch_station_group_limit = _args.fetch_station_group_limit
cadence_seconds = _args.cadence_seconds

if __name__ == "__main__":
    print(f"Starting ingestion service with fetch_station_group_limit={fetch_station_group_limit} and cadence_seconds={cadence_seconds}...")

    if not os.path.exists("ingestion_data/zones/"):
        write_all_zones(read_all_zones(fetch_all_zones()))

    next_station_group_page = None
    if not os.path.exists("ingestion_data/stations/"):
        for _ in range(2):
            next_station_group_page = write_station_group(read_station_group(fetch_station_group(limit = fetch_station_group_limit, cursor = next_station_group_page)))

    conn = duckdb.connect()
    stations_list = conn.execute("SELECT DISTINCT station_id FROM read_parquet('ingestion_data/stations/station_group_*.parquet')").fetchall()

    while True:
        start_time = datetime.now()
        print(f"Fetching observations at {start_time}...")
        
        for station_id in stations_list:
            write_latest_observation(read_latest_observation(fetch_latest_observation_for_station(station_id[0])))
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        sleep_time = max(0, cadence_seconds - duration)
        if sleep_time > 0:
            print(f"Fetch completed in {duration:.2f} seconds. Sleeping for {sleep_time:.2f} seconds...")
            import time
            time.sleep(sleep_time)