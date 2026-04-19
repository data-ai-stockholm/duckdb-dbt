"""Fetch weather station data from the API and store it in Parquet files."""

from pathlib import Path

import duckdb


def main():
    """Fetch weather station metadata from the National Weather Service API."""
    # Create output directory
    Path("ingestion_data/stations").mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()
    next_page = "https://api.weather.gov/stations"

    print("Fetching weather stations...")
    page_count = 0

    while True:
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE weather_stations AS
                SELECT
                    UNNEST(observationStations) AS station_url,
                    pagination.next AS next_page
                FROM read_json_auto('{next_page}');
            """)

            next_page_result = conn.execute("""
                SELECT DISTINCT next_page
                FROM weather_stations
            """).fetchone()

            if not next_page_result or not next_page_result[0]:
                print("No more pages to fetch")
                break

            next_page = next_page_result[0]
            cursor = next_page.split("cursor=")[-1]

            conn.execute(f"""
                COPY (FROM weather_stations)
                TO 'ingestion_data/stations/weather_stations_{cursor}.parquet'
                (FORMAT PARQUET)
            """).df()

            page_count += 1
            print(f"✓ Saved page {page_count}: cursor={cursor}")

        except KeyboardInterrupt:
            print(f"\n\nInterrupted by user. Saved {page_count} pages.")
            break
        except Exception as e:
            print(f"Error fetching page: {e}")
            break

    conn.close()
    print(f"\n✓ Completed! Fetched {page_count} pages of station data")
    print("  Data saved to: ingestion_data/stations/")


if __name__ == "__main__":
    main()
