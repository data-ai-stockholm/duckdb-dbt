"""Generate synthetic weather observation data at configurable scale.

Produces realistic weather patterns with seasonal variation, station-specific
characteristics, and correlated measurements (temperature <-> humidity, etc.).
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

# Column definitions: (name, duckdb_type, arrow_type_key)
# arrow_type_key: "ts" for timestamp, "f64" for float64, "str" for string
COLUMNS = [
    ("observation_timestamp", "TIMESTAMPTZ", "ts"),
    ("observation_id", "VARCHAR", "str"),
    ("observation_type", "VARCHAR", "str"),
    ("geometry_type", "VARCHAR", "str"),
    ("geometry_coordinates", "VARCHAR", "str"),
    ("elevation_m", "DOUBLE", "f64"),
    ("station_id", "VARCHAR", "str"),
    ("station_name", "VARCHAR", "str"),
    ("temperature_degC", "DOUBLE", "f64"),
    ("dewpoint_degC", "DOUBLE", "f64"),
    ("wind_direction_deg", "DOUBLE", "f64"),
    ("wind_speed_kmh", "DOUBLE", "f64"),
    ("wind_gust_kmh", "DOUBLE", "f64"),
    ("barometric_pressure_Pa", "DOUBLE", "f64"),
    ("sea_level_pressure_Pa", "DOUBLE", "f64"),
    ("visibility_m", "DOUBLE", "f64"),
    ("max_temp_24h_degC", "DOUBLE", "f64"),
    ("min_temp_24h_degC", "DOUBLE", "f64"),
    ("precipitation_3h_mm", "DOUBLE", "f64"),
    ("relative_humidity_percent", "DOUBLE", "f64"),
    ("wind_chill_degC", "DOUBLE", "f64"),
    ("heat_index_degC", "DOUBLE", "f64"),
]

COL_NAMES = [c[0] for c in COLUMNS]


def generate_observations(num_rows: int, seed: int = 42) -> list:
    """Generate synthetic weather observations with realistic distributions."""
    rng = np.random.default_rng(seed)

    # Distribute rows across stations
    station_weights = rng.dirichlet(np.ones(len(STATIONS)) * 5)
    station_counts = (station_weights * num_rows).astype(int)
    station_counts[-1] = num_rows - station_counts[:-1].sum()

    hours_span = num_rows // len(STATIONS)
    base_ts = np.datetime64("2023-01-01T00:00")
    all_data = []

    for station, count in zip(STATIONS, station_counts):
        offsets = np.sort(rng.choice(hours_span * 2, size=count, replace=False))
        timestamps = base_ts + offsets.astype("timedelta64[h]")
        hour_of_day = (offsets % 24).astype(float)
        day_of_year = ((offsets // 24) % 365).astype(float)

        # Temperature: seasonal + diurnal + noise
        seasonal = station["temp_range"] * np.sin(2 * np.pi * (day_of_year - 80) / 365)
        diurnal = 3.0 * np.sin(2 * np.pi * (hour_of_day - 6) / 24)
        temperature = station["base_temp"] + seasonal + diurnal + rng.normal(0, 2.5, count)

        # Correlated measurements
        humidity = np.clip(65 - 0.8 * (temperature - station["base_temp"]) + rng.normal(0, 10, count), 10, 100)
        wind_speed = np.clip(rng.lognormal(2.0, 0.6, count), 0, 80)
        wind_direction = (rng.uniform(0, 360) + rng.vonmises(0, 2, count) * 180 / np.pi) % 360
        wind_gust = np.where(rng.random(count) < 0.3, wind_speed * rng.uniform(1.3, 2.5, count), np.nan)
        elevation_correction = station["elev"] * 0.12
        pressure = rng.normal(101325 - elevation_correction * 100, 800, count)
        sea_level_pressure = pressure + elevation_correction * 100
        visibility = np.where(rng.random(count) < 0.1, rng.uniform(100, 3000, count), rng.uniform(8000, 16000, count))
        dewpoint = temperature - ((100 - humidity) / 5)
        precipitation = np.where(rng.random(count) < 0.15, rng.exponential(3.0, count), 0.0)

        for j in range(count):
            all_data.append((
                str(timestamps[j]),
                f"obs-{station['id']}-{j:08d}",
                "wx:ObservationStation", "Point",
                f"[{station['lon']}, {station['lat']}]",
                station["elev"], station["id"], station["name"],
                round(float(temperature[j]), 2),
                round(float(dewpoint[j]), 2),
                round(float(wind_direction[j]), 1),
                round(float(wind_speed[j]) * 3.6, 2),
                round(float(wind_gust[j]) * 3.6, 2) if not np.isnan(wind_gust[j]) else None,
                round(float(pressure[j]), 1),
                round(float(sea_level_pressure[j]), 1),
                round(float(visibility[j]), 0),
                None, None,  # max/min temp 24h
                round(float(precipitation[j]), 2) if precipitation[j] > 0 else None,
                round(float(humidity[j]), 1),
                None, None,  # wind_chill, heat_index
            ))

    return all_data


def write_parquet(data: list, output_path: Path) -> None:
    """Write generated data directly to Parquet using DuckDB."""
    conn = duckdb.connect()
    schema_sql = ", ".join(f"{name} {dtype}" for name, dtype, _ in COLUMNS)
    conn.execute(f"CREATE TABLE observations ({schema_sql})")

    placeholders = ",".join(["?"] * len(COLUMNS))
    batch_size = 50_000
    for i in range(0, len(data), batch_size):
        conn.executemany(f"INSERT INTO observations VALUES ({placeholders})", data[i:i + batch_size])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(f"COPY observations TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    conn.close()
    print(f"  Written {count:,} rows to {output_path}")


def write_iceberg(data: list, warehouse_path: Path) -> None:
    """Write generated data to Iceberg table using PyIceberg."""
    import pandas as pd
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

    warehouse_path.mkdir(parents=True, exist_ok=True)

    catalog = SqlCatalog(
        "benchmark",
        **{"uri": f"sqlite:///{warehouse_path / 'catalog.db'}", "warehouse": str(warehouse_path)},
    )

    try:
        catalog.create_namespace("benchmark")
    except Exception:
        pass

    # Build Iceberg schema from COLUMNS definition
    _iceberg_types = {"ts": TimestamptzType(), "f64": DoubleType(), "str": StringType()}
    iceberg_schema = Schema(*[
        NestedField(i + 1, name, _iceberg_types[atype], required=False)
        for i, (name, _, atype) in enumerate(COLUMNS)
    ])

    try:
        catalog.drop_table("benchmark.observations")
    except Exception:
        pass
    table = catalog.create_table("benchmark.observations", schema=iceberg_schema)

    # Build PyArrow table from row data
    col_data = {name: [] for name, _, _ in COLUMNS}
    for row in data:
        for i, (name, _, _) in enumerate(COLUMNS):
            col_data[name].append(row[i])

    _arrow_types = {"ts": pa.timestamp("us", tz="UTC"), "f64": pa.float64(), "str": pa.string()}
    timestamps = pd.to_datetime(col_data[COL_NAMES[0]], utc=True)

    arrow_arrays = [pa.array(timestamps.values, type=pa.timestamp("us", tz="UTC"))]
    arrow_arrays += [
        pa.array(col_data[name], type=_arrow_types[atype])
        for name, _, atype in COLUMNS[1:]
    ]
    arrow_schema = pa.schema([(name, _arrow_types[atype]) for name, _, atype in COLUMNS])
    arrow_table = pa.table(arrow_arrays, schema=arrow_schema)

    table.append(arrow_table)

    # Create version-hint and DuckDB-compatible symlink
    table_location = table.location()
    metadata_dir = Path(table_location.replace("file://", "")) / "metadata"
    if not metadata_dir.exists():
        metadata_dir = warehouse_path / "benchmark" / "observations" / "metadata"

    metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
    if metadata_files:
        version_int = int(metadata_files[-1].stem.split("-")[0]) + 1
        (metadata_dir / "version-hint.text").write_text(str(version_int))
        # DuckDB expects v{N}.metadata.json but PyIceberg uses {N}-{uuid}.metadata.json
        symlink = metadata_dir / f"v{version_int}.metadata.json"
        if symlink.exists():
            symlink.unlink()
        symlink.symlink_to(metadata_files[-1].name)

    count = table.scan().to_arrow().num_rows
    print(f"  Written {count:,} rows to Iceberg at {warehouse_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic weather data for benchmarking")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows (default: 1M)")
    parser.add_argument("--output-dir", type=str, default="benchmarks/data", help="Output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    print(f"\nGenerating {args.rows:,} synthetic weather observations...")
    print(f"  Stations: {len(STATIONS)}, Seed: {args.seed}\n")

    data = generate_observations(args.rows, seed=args.seed)
    print(f"  Generated {len(data):,} records in memory\n")

    print("Writing Parquet format...")
    write_parquet(data, output_dir / "observations.parquet")

    print("\nWriting Iceberg format...")
    write_iceberg(data, output_dir / "iceberg_warehouse")

    print(f"\nDone! Data written to {output_dir}/")
    print("  - observations.parquet (raw Parquet)")
    print("  - iceberg_warehouse/  (Iceberg table)")


if __name__ == "__main__":
    main()