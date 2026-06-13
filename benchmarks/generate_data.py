"""Generate synthetic weather observation data at configurable scale.

Produces realistic weather patterns with seasonal variation, station-specific
characteristics, and correlated measurements (temperature ↔ humidity, etc.).
"""

import argparse
from pathlib import Path

import duckdb
import numpy as np


# Station definitions with realistic base conditions
STATIONS = [
    {"id": "KJFK", "name": "JFK International", "lat": 40.64, "lon": -73.78, "elev": 4.0,
     "base_temp": 12.0, "temp_range": 15.0},
    {"id": "KLAX", "name": "Los Angeles Intl", "lat": 33.94, "lon": -118.41, "elev": 38.0,
     "base_temp": 18.0, "temp_range": 8.0},
    {"id": "KORD", "name": "Chicago O'Hare", "lat": 41.97, "lon": -87.91, "elev": 205.0,
     "base_temp": 10.0, "temp_range": 18.0},
    {"id": "KDEN", "name": "Denver International", "lat": 39.86, "lon": -104.67, "elev": 1655.0,
     "base_temp": 10.5, "temp_range": 16.0},
    {"id": "KMIA", "name": "Miami International", "lat": 25.79, "lon": -80.29, "elev": 2.4,
     "base_temp": 25.0, "temp_range": 6.0},
    {"id": "KSEA", "name": "Seattle-Tacoma", "lat": 47.45, "lon": -122.31, "elev": 137.0,
     "base_temp": 11.0, "temp_range": 9.0},
    {"id": "KATL", "name": "Atlanta Hartsfield", "lat": 33.63, "lon": -84.44, "elev": 315.0,
     "base_temp": 17.0, "temp_range": 12.0},
    {"id": "KDFW", "name": "Dallas-Fort Worth", "lat": 32.90, "lon": -97.04, "elev": 171.0,
     "base_temp": 19.0, "temp_range": 14.0},
    {"id": "KPHX", "name": "Phoenix Sky Harbor", "lat": 33.44, "lon": -112.01, "elev": 337.0,
     "base_temp": 24.0, "temp_range": 16.0},
    {"id": "KBOS", "name": "Boston Logan", "lat": 42.36, "lon": -71.01, "elev": 6.0,
     "base_temp": 11.0, "temp_range": 15.0},
]


def generate_observations(num_rows: int, seed: int = 42) -> np.ndarray:
    """Generate synthetic weather observations with realistic distributions.

    Args:
        num_rows: Total number of observation rows to generate.
        seed: Random seed for reproducibility.

    Returns:
        Structured numpy array with observation data.
    """
    rng = np.random.default_rng(seed)

    # Distribute rows across stations (roughly equal, some variation)
    station_weights = rng.dirichlet(np.ones(len(STATIONS)) * 5)
    station_counts = (station_weights * num_rows).astype(int)
    station_counts[-1] = num_rows - station_counts[:-1].sum()  # fix rounding

    # Generate timestamps spanning 2 years of hourly observations
    hours_span = num_rows // len(STATIONS)
    base_ts = np.datetime64("2023-01-01T00:00")

    all_data = []

    for i, (station, count) in enumerate(zip(STATIONS, station_counts)):
        # Timestamps with some jitter (not perfectly hourly)
        offsets = np.sort(rng.choice(hours_span * 2, size=count, replace=False))
        timestamps = base_ts + offsets.astype("timedelta64[h]")

        # Hour of day for diurnal cycle
        hour_of_day = (offsets % 24).astype(float)
        # Day of year for seasonal cycle
        day_of_year = ((offsets // 24) % 365).astype(float)

        # Temperature: seasonal + diurnal + noise
        seasonal = station["temp_range"] * np.sin(2 * np.pi * (day_of_year - 80) / 365)
        diurnal = 3.0 * np.sin(2 * np.pi * (hour_of_day - 6) / 24)
        noise = rng.normal(0, 2.5, count)
        temperature = station["base_temp"] + seasonal + diurnal + noise

        # Humidity: inversely correlated with temperature
        base_humidity = 65 - 0.8 * (temperature - station["base_temp"])
        humidity = np.clip(base_humidity + rng.normal(0, 10, count), 10, 100)

        # Wind speed: log-normal distribution
        wind_speed = rng.lognormal(mean=2.0, sigma=0.6, size=count)
        wind_speed = np.clip(wind_speed, 0, 80)

        # Wind direction: clustered around prevailing direction with spread
        prevailing = rng.uniform(0, 360)
        wind_direction = (prevailing + rng.vonmises(0, 2, count) * 180 / np.pi) % 360

        # Wind gust: wind_speed * multiplier (sometimes None)
        gust_mask = rng.random(count) < 0.3  # 30% have gusts
        wind_gust = np.where(gust_mask, wind_speed * rng.uniform(1.3, 2.5, count), np.nan)

        # Pressure: normal distribution around sea level, adjusted for elevation
        elevation_correction = station["elev"] * 0.12  # ~0.12 hPa per meter
        pressure = rng.normal(101325 - elevation_correction * 100, 800, count)

        # Sea level pressure
        sea_level_pressure = pressure + elevation_correction * 100

        # Visibility: usually good, sometimes poor (bimodal)
        visibility = np.where(
            rng.random(count) < 0.1,
            rng.uniform(100, 3000, count),    # poor visibility 10%
            rng.uniform(8000, 16000, count),  # good visibility 90%
        )

        # Dewpoint: derived from temperature and humidity
        # Magnus formula approximation
        dewpoint = temperature - ((100 - humidity) / 5)

        # Precipitation (3h): mostly zero, occasionally some
        precip_mask = rng.random(count) < 0.15
        precipitation = np.where(precip_mask, rng.exponential(3.0, count), 0.0)

        # Build records for this station
        for j in range(count):
            all_data.append((
                str(timestamps[j]),
                f"obs-{station['id']}-{j:08d}",
                "wx:ObservationStation",
                "Point",
                f"[{station['lon']}, {station['lat']}]",
                station["elev"],
                station["id"],
                station["name"],
                round(float(temperature[j]), 2),
                round(float(dewpoint[j]), 2),
                round(float(wind_direction[j]), 1),
                round(float(wind_speed[j]) * 3.6, 2),  # m/s to km/h
                round(float(wind_gust[j]) * 3.6, 2) if not np.isnan(wind_gust[j]) else None,
                round(float(pressure[j]), 1),
                round(float(sea_level_pressure[j]), 1),
                round(float(visibility[j]), 0),
                None,  # max_temp_24h
                None,  # min_temp_24h
                round(float(precipitation[j]), 2) if precipitation[j] > 0 else None,
                round(float(humidity[j]), 1),
                None,  # wind_chill
                None,  # heat_index
            ))

    return all_data


def write_parquet(data: list, output_path: Path) -> None:
    """Write generated data directly to Parquet using DuckDB."""
    conn = duckdb.connect()

    conn.execute("""
        CREATE TABLE observations (
            observation_timestamp TIMESTAMPTZ,
            observation_id VARCHAR,
            observation_type VARCHAR,
            geometry_type VARCHAR,
            geometry_coordinates VARCHAR,
            elevation_m DOUBLE,
            station_id VARCHAR,
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
        )
    """)

    # Insert in batches for memory efficiency
    batch_size = 50_000
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        conn.executemany("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(f"COPY observations TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")

    count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    conn.close()
    print(f"  Written {count:,} rows to {output_path}")


def write_iceberg(data: list, warehouse_path: Path) -> None:
    """Write generated data to Iceberg table using PyIceberg + Parquet files.

    Creates a proper Iceberg table with metadata that DuckDB can read via iceberg_scan.
    """
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DoubleType,
        NestedField,
        StringType,
        TimestamptzType,
    )

    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog_db = warehouse_path / "catalog.db"

    # Create a SQL catalog backed by SQLite (local, no REST server needed)
    catalog = SqlCatalog(
        "benchmark",
        **{
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": str(warehouse_path),
        },
    )

    # Create namespace
    try:
        catalog.create_namespace("benchmark")
    except Exception:
        pass  # Already exists

    # Define Iceberg schema
    iceberg_schema = Schema(
        NestedField(1, "observation_timestamp", TimestamptzType(), required=False),
        NestedField(2, "observation_id", StringType(), required=False),
        NestedField(3, "observation_type", StringType(), required=False),
        NestedField(4, "geometry_type", StringType(), required=False),
        NestedField(5, "geometry_coordinates", StringType(), required=False),
        NestedField(6, "elevation_m", DoubleType(), required=False),
        NestedField(7, "station_id", StringType(), required=False),
        NestedField(8, "station_name", StringType(), required=False),
        NestedField(9, "temperature_degC", DoubleType(), required=False),
        NestedField(10, "dewpoint_degC", DoubleType(), required=False),
        NestedField(11, "wind_direction_deg", DoubleType(), required=False),
        NestedField(12, "wind_speed_kmh", DoubleType(), required=False),
        NestedField(13, "wind_gust_kmh", DoubleType(), required=False),
        NestedField(14, "barometric_pressure_Pa", DoubleType(), required=False),
        NestedField(15, "sea_level_pressure_Pa", DoubleType(), required=False),
        NestedField(16, "visibility_m", DoubleType(), required=False),
        NestedField(17, "max_temp_24h_degC", DoubleType(), required=False),
        NestedField(18, "min_temp_24h_degC", DoubleType(), required=False),
        NestedField(19, "precipitation_3h_mm", DoubleType(), required=False),
        NestedField(20, "relative_humidity_percent", DoubleType(), required=False),
        NestedField(21, "wind_chill_degC", DoubleType(), required=False),
        NestedField(22, "heat_index_degC", DoubleType(), required=False),
    )

    # Create or replace table
    try:
        catalog.drop_table("benchmark.observations")
    except Exception:
        pass
    table = catalog.create_table("benchmark.observations", schema=iceberg_schema)

    # Convert data to PyArrow table
    columns = [
        "observation_timestamp", "observation_id", "observation_type",
        "geometry_type", "geometry_coordinates", "elevation_m",
        "station_id", "station_name", "temperature_degC", "dewpoint_degC",
        "wind_direction_deg", "wind_speed_kmh", "wind_gust_kmh",
        "barometric_pressure_Pa", "sea_level_pressure_Pa", "visibility_m",
        "max_temp_24h_degC", "min_temp_24h_degC", "precipitation_3h_mm",
        "relative_humidity_percent", "wind_chill_degC", "heat_index_degC",
    ]

    # Build columnar data from row-based data
    col_data = {col: [] for col in columns}
    for row in data:
        for i, col in enumerate(columns):
            col_data[col].append(row[i])

    # Parse timestamps
    import pandas as pd
    timestamps = pd.to_datetime(col_data["observation_timestamp"], utc=True)

    arrow_schema = pa.schema([
        ("observation_timestamp", pa.timestamp("us", tz="UTC")),
        ("observation_id", pa.string()),
        ("observation_type", pa.string()),
        ("geometry_type", pa.string()),
        ("geometry_coordinates", pa.string()),
        ("elevation_m", pa.float64()),
        ("station_id", pa.string()),
        ("station_name", pa.string()),
        ("temperature_degC", pa.float64()),
        ("dewpoint_degC", pa.float64()),
        ("wind_direction_deg", pa.float64()),
        ("wind_speed_kmh", pa.float64()),
        ("wind_gust_kmh", pa.float64()),
        ("barometric_pressure_Pa", pa.float64()),
        ("sea_level_pressure_Pa", pa.float64()),
        ("visibility_m", pa.float64()),
        ("max_temp_24h_degC", pa.float64()),
        ("min_temp_24h_degC", pa.float64()),
        ("precipitation_3h_mm", pa.float64()),
        ("relative_humidity_percent", pa.float64()),
        ("wind_chill_degC", pa.float64()),
        ("heat_index_degC", pa.float64()),
    ])

    arrow_arrays = [
        pa.array(timestamps.values, type=pa.timestamp("us", tz="UTC")),
    ]
    for col in columns[1:]:
        if col.endswith(("_m", "_degC", "_deg", "_kmh", "_Pa", "_mm", "_percent")):
            arrow_arrays.append(pa.array(col_data[col], type=pa.float64()))
        else:
            arrow_arrays.append(pa.array(col_data[col], type=pa.string()))

    arrow_table = pa.table(arrow_arrays, schema=arrow_schema)

    # Write to Iceberg table
    table.append(arrow_table)

    # Write version-hint.text so DuckDB iceberg_scan can find the latest metadata
    table_location = table.location()
    metadata_dir = Path(table_location.replace("file://", "")) / "metadata"
    if not metadata_dir.exists():
        # PyIceberg may use a relative path
        metadata_dir = warehouse_path / "benchmark" / "observations" / "metadata"

    metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
    if metadata_files:
        latest_version = metadata_files[-1].stem.split("-")[0]  # e.g. "00001"
        version_int = int(latest_version) + 1
        version_hint = metadata_dir / "version-hint.text"
        version_hint.write_text(str(version_int))

        # Create v{N}.metadata.json symlink for DuckDB iceberg_scan compatibility
        # DuckDB expects v{version}.metadata.json but PyIceberg uses {N}-{uuid}.metadata.json
        symlink_name = metadata_dir / f"v{version_int}.metadata.json"
        if symlink_name.exists():
            symlink_name.unlink()
        symlink_name.symlink_to(metadata_files[-1].name)

    # Verify
    scan = table.scan()
    count = scan.to_arrow().num_rows
    print(f"  Written {count:,} rows to Iceberg at {warehouse_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic weather data for benchmarking")
    parser.add_argument(
        "--rows", type=int, default=1_000_000,
        help="Number of rows to generate (default: 1,000,000)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="benchmarks/data",
        help="Output directory for generated data",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    print(f"\nGenerating {args.rows:,} synthetic weather observations...")
    print(f"  Stations: {len(STATIONS)}")
    print(f"  Seed: {args.seed}")
    print()

    data = generate_observations(args.rows, seed=args.seed)
    print(f"  Generated {len(data):,} records in memory\n")

    # Write Parquet format
    print("Writing Parquet format...")
    write_parquet(data, output_dir / "observations.parquet")

    # Write Iceberg format
    print("\nWriting Iceberg format...")
    write_iceberg(data, output_dir / "iceberg_warehouse")

    print(f"\nDone! Data written to {output_dir}/")
    print("  - observations.parquet (raw Parquet)")
    print("  - iceberg_warehouse/  (Iceberg table)")


if __name__ == "__main__":
    main()
