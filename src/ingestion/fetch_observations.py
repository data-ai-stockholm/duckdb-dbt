"""Fetch weather observations for each station and store them in Parquet files."""

from pathlib import Path

import duckdb


def main():
    """Fetch weather observations for all stations."""
    # Check if station data exists
    station_files = list(Path("ingestion_data/stations").glob("weather_stations_*.parquet"))
    if not station_files:
        print("⚠ No station data found!")
        print("  Please run fetch_stations.py first")
        return

    # Create output directory
    Path("ingestion_data/observations").mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    # Get list of station URLs
    print("Loading station URLs...")
    station_urls = (
        conn.execute("""
        SELECT station_url
        FROM read_parquet('ingestion_data/stations/weather_stations_*.parquet')
    """)
        .df()["station_url"]
        .tolist()
    )

    print(f"Found {len(station_urls)} stations to fetch")
    print("Starting observation fetch (this may take several hours)...")
    print("Press Ctrl+C to stop at any time\n")

    fetched_count = 0
    error_count = 0

    for i, url in enumerate(station_urls, 1):
        try:
            station_id = url.split("/")[-1]
            output_file = f"ingestion_data/observations/observations_station_{station_id}.parquet"

            # Check if already fetched
            if Path(output_file).exists():
                print(f"[{i}/{len(station_urls)}] ⏭  Skipping {station_id} (already exists)")
                fetched_count += 1
                continue

            conn.execute(f"""
                COPY (
                    SELECT
                        UNNEST(features) AS observation
                    FROM read_json_auto('{url}/observations')
                ) TO '{output_file}' (FORMAT PARQUET)
            """)

            fetched_count += 1
            if fetched_count % 10 == 0:
                print(
                    f"[{i}/{len(station_urls)}] ✓ Fetched {fetched_count} stations ({error_count} errors)"
                )
            else:
                print(f"[{i}/{len(station_urls)}] ✓ {station_id}")

        except KeyboardInterrupt:
            print("\n\n⚠ Interrupted by user")
            print(f"  Successfully fetched: {fetched_count} stations")
            print(f"  Errors: {error_count}")
            print("  You can run this script again to continue from where you left off")
            break
        except Exception as e:
            error_count += 1
            print(f"[{i}/{len(station_urls)}] ✗ Error fetching {url.split('/')[-1]}: {e}")

    conn.close()
    print(f"\n{'=' * 70}")
    print("✓ Fetch complete!")
    print(f"  Successfully fetched: {fetched_count} stations")
    print(f"  Errors: {error_count}")
    print("  Data saved to: ingestion_data/observations/")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
