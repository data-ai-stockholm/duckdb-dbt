"""Benchmark runner: compare query performance between Iceberg and Parquet.

Runs identical analytical queries against both storage formats, measures
execution time over multiple iterations, and reports statistics.
"""

import argparse
import json
import time
from pathlib import Path
from statistics import mean, median, stdev

import duckdb

# Benchmark queries modeled after the dbt transformations in this project
QUERIES = {
    "full_scan": {
        "description": "Full table scan — read all columns, all rows",
        "sql": "SELECT * FROM {table}",
    },
    "count_star": {
        "description": "Simple count — metadata-only if supported",
        "sql": "SELECT COUNT(*) FROM {table}",
    },
    "filtered_scan": {
        "description": "Point query — filter by station and date range",
        "sql": """
            SELECT *
            FROM {table}
            WHERE station_id = 'KJFK'
              AND observation_timestamp >= '2023-06-01'
              AND observation_timestamp < '2023-07-01'
        """,
    },
    "daily_aggregation": {
        "description": "Daily weather stats — mirrors fact_daily_weather model",
        "sql": """
            SELECT
                station_id,
                DATE_TRUNC('day', observation_timestamp) AS observation_date,
                COUNT(*) AS observation_count,
                ROUND(AVG(temperature_degC), 2) AS avg_temp,
                ROUND(MIN(temperature_degC), 2) AS min_temp,
                ROUND(MAX(temperature_degC), 2) AS max_temp,
                ROUND(AVG(relative_humidity_percent), 2) AS avg_humidity,
                ROUND(AVG(wind_speed_kmh), 2) AS avg_wind,
                ROUND(MAX(wind_speed_kmh), 2) AS max_wind,
                ROUND(AVG(barometric_pressure_Pa), 2) AS avg_pressure
            FROM {table}
            GROUP BY station_id, DATE_TRUNC('day', observation_timestamp)
        """,
    },
    "anomaly_detection": {
        "description": "Z-score anomaly detection — mirrors extreme_weather_events model",
        "sql": """
            WITH station_stats AS (
                SELECT
                    station_id,
                    AVG(temperature_degC) AS avg_temp,
                    STDDEV(temperature_degC) AS stddev_temp
                FROM {table}
                GROUP BY station_id
            )
            SELECT
                o.observation_id,
                o.observation_timestamp,
                o.station_id,
                o.temperature_degC,
                s.avg_temp,
                s.stddev_temp,
                ABS(o.temperature_degC - s.avg_temp) / NULLIF(s.stddev_temp, 0) AS temp_z_score
            FROM {table} o
            JOIN station_stats s ON o.station_id = s.station_id
            WHERE ABS(o.temperature_degC - s.avg_temp) / NULLIF(s.stddev_temp, 0) > 2
        """,
    },
    "station_dimension": {
        "description": "Station dimension build — mirrors dim_stations model",
        "sql": """
            SELECT
                station_id,
                station_name,
                elevation_m,
                COUNT(*) AS total_observations,
                MIN(observation_timestamp) AS first_observation,
                MAX(observation_timestamp) AS last_observation,
                AVG(temperature_degC) AS avg_temp
            FROM {table}
            GROUP BY station_id, station_name, elevation_m
        """,
    },
    "window_function": {
        "description": "Window function — running average per station",
        "sql": """
            SELECT
                station_id,
                observation_timestamp,
                temperature_degC,
                AVG(temperature_degC) OVER (
                    PARTITION BY station_id
                    ORDER BY observation_timestamp
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) AS rolling_24h_avg
            FROM {table}
            WHERE station_id IN ('KJFK', 'KLAX', 'KORD')
        """,
    },
    "multi_join_aggregation": {
        "description": "Complex aggregation — percentiles and conditional stats",
        "sql": """
            SELECT
                station_id,
                EXTRACT(MONTH FROM observation_timestamp) AS month,
                COUNT(*) AS obs_count,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temperature_degC) AS median_temp,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY wind_speed_kmh) AS p95_wind,
                SUM(CASE WHEN precipitation_3h_mm > 0 THEN 1 ELSE 0 END) AS precip_hours,
                AVG(CASE WHEN relative_humidity_percent > 80 THEN temperature_degC END) AS avg_temp_humid
            FROM {table}
            GROUP BY station_id, EXTRACT(MONTH FROM observation_timestamp)
        """,
    },
}


def setup_parquet_connection(data_dir: Path) -> tuple[duckdb.DuckDBPyConnection, str]:
    """Set up DuckDB connection for Parquet queries."""
    conn = duckdb.connect()
    parquet_path = data_dir / "observations.parquet"
    table_ref = f"read_parquet('{parquet_path}')"
    return conn, table_ref


def setup_iceberg_connection(data_dir: Path) -> tuple[duckdb.DuckDBPyConnection, str]:
    """Set up DuckDB connection for Iceberg queries via iceberg_scan."""
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    # Find the Iceberg table root directory
    warehouse_path = data_dir / "iceberg_warehouse"
    table_path = warehouse_path / "benchmark" / "observations"

    if not table_path.exists():
        # Search for any table with metadata
        import glob
        metadata_files = sorted(glob.glob(str(warehouse_path / "**" / "metadata" / "*.metadata.json"), recursive=True))
        if not metadata_files:
            raise FileNotFoundError(f"No Iceberg metadata found in {warehouse_path}")
        # Table root is two levels up from metadata file
        table_path = Path(metadata_files[-1]).parent.parent

    # Use iceberg_scan with the table root path
    # Enable version guessing in case version-hint.text is missing
    conn.execute("SET unsafe_enable_version_guessing = true;")
    table_ref = f"iceberg_scan('{table_path}', allow_moved_paths = true)"
    return conn, table_ref


def run_query(conn: duckdb.DuckDBPyConnection, sql: str) -> float:
    """Execute a query and return elapsed wall-clock time in milliseconds."""
    start = time.perf_counter()
    conn.execute(sql).fetchall()
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed


def run_benchmarks(
    data_dir: Path,
    iterations: int = 5,
    warmup: int = 1,
    queries: list[str] | None = None,
) -> dict:
    """Run benchmark suite comparing Iceberg vs Parquet.

    Args:
        data_dir: Directory containing benchmark data.
        iterations: Number of timed iterations per query.
        warmup: Number of warmup iterations (not counted).
        queries: Specific query names to run (None = all).

    Returns:
        Dictionary with benchmark results.
    """
    query_set = {k: v for k, v in QUERIES.items() if queries is None or k in queries}

    print(f"\nBenchmark Configuration:")
    print(f"  Data directory: {data_dir}")
    print(f"  Iterations: {iterations} (+ {warmup} warmup)")
    print(f"  Queries: {len(query_set)}")
    print()

    # Setup connections
    print("Setting up Parquet connection...")
    parquet_conn, parquet_table = setup_parquet_connection(data_dir)

    print("Setting up Iceberg connection...")
    iceberg_conn, iceberg_table = setup_iceberg_connection(data_dir)

    # Verify row counts match
    parquet_count = parquet_conn.execute(f"SELECT COUNT(*) FROM {parquet_table}").fetchone()[0]
    iceberg_count = iceberg_conn.execute(f"SELECT COUNT(*) FROM {iceberg_table}").fetchone()[0]
    print(f"\n  Parquet rows: {parquet_count:,}")
    print(f"  Iceberg rows: {iceberg_count:,}")

    if parquet_count != iceberg_count:
        print("  WARNING: Row counts differ!")

    results = {}

    print(f"\n{'=' * 80}")
    print(f"{'Query':<25} {'Format':<10} {'Median (ms)':<14} {'Mean (ms)':<12} {'P95 (ms)':<12} {'StdDev':<10}")
    print(f"{'=' * 80}")

    for query_name, query_info in query_set.items():
        parquet_sql = query_info["sql"].format(table=parquet_table)
        iceberg_sql = query_info["sql"].format(table=iceberg_table)

        # Warmup
        for _ in range(warmup):
            run_query(parquet_conn, parquet_sql)
            run_query(iceberg_conn, iceberg_sql)

        # Timed runs
        parquet_times = [run_query(parquet_conn, parquet_sql) for _ in range(iterations)]
        iceberg_times = [run_query(iceberg_conn, iceberg_sql) for _ in range(iterations)]

        parquet_stats = _compute_stats(parquet_times)
        iceberg_stats = _compute_stats(iceberg_times)

        results[query_name] = {
            "description": query_info["description"],
            "parquet": {**parquet_stats, "raw_times": parquet_times},
            "iceberg": {**iceberg_stats, "raw_times": iceberg_times},
            "overhead_pct": round(
                (iceberg_stats["median"] - parquet_stats["median"]) / parquet_stats["median"] * 100, 1
            ),
        }

        # Print results
        overhead = results[query_name]["overhead_pct"]
        for fmt, stats in [("Parquet", parquet_stats), ("Iceberg", iceberg_stats)]:
            label = query_name if fmt == "Parquet" else ""
            print(f"{label:<25} {fmt:<10} {stats['median']:<14.2f} {stats['mean']:<12.2f} {stats['p95']:<12.2f} {stats['stddev']:<10.2f}")
        print(f"{'':<25} {'Δ':<10} {'+' if overhead > 0 else ''}{overhead}%")
        print(f"{'-' * 80}")

    parquet_conn.close()
    iceberg_conn.close()

    return results


def _compute_stats(times: list[float]) -> dict:
    """Compute summary statistics for a list of timings."""
    sorted_times = sorted(times)
    p95_idx = int(len(sorted_times) * 0.95)
    return {
        "median": median(times),
        "mean": round(mean(times), 2),
        "min": round(min(times), 2),
        "max": round(max(times), 2),
        "p95": sorted_times[min(p95_idx, len(sorted_times) - 1)],
        "stddev": round(stdev(times), 2) if len(times) > 1 else 0.0,
    }


def print_summary(results: dict) -> None:
    """Print a summary comparison table."""
    print(f"\n{'=' * 60}")
    print("SUMMARY: Iceberg overhead vs raw Parquet")
    print(f"{'=' * 60}")
    print(f"{'Query':<25} {'Overhead':<12} {'Verdict'}")
    print(f"{'-' * 60}")

    for name, r in results.items():
        overhead = r["overhead_pct"]
        if overhead < -5:
            verdict = "Iceberg FASTER"
        elif overhead < 5:
            verdict = "~Same"
        elif overhead < 20:
            verdict = "Slight overhead"
        else:
            verdict = "Significant overhead"

        indicator = "+" if overhead > 0 else ""
        print(f"{name:<25} {indicator}{overhead:>5}%      {verdict}")

    avg_overhead = round(mean(r["overhead_pct"] for r in results.values()), 1)
    print(f"{'-' * 60}")
    print(f"{'Average overhead':<25} {'+' if avg_overhead > 0 else ''}{avg_overhead}%")
    print()


def main():
    parser = argparse.ArgumentParser(description="Run Iceberg vs Parquet benchmarks")
    parser.add_argument(
        "--data-dir", type=str, default="benchmarks/data",
        help="Directory containing benchmark data",
    )
    parser.add_argument(
        "--iterations", type=int, default=5,
        help="Number of timed iterations per query (default: 5)",
    )
    parser.add_argument(
        "--warmup", type=int, default=1,
        help="Warmup iterations before timing (default: 1)",
    )
    parser.add_argument(
        "--queries", type=str, nargs="*", default=None,
        help="Specific queries to run (default: all)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Save results to JSON file",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not (data_dir / "observations.parquet").exists():
        print(f"ERROR: No benchmark data found at {data_dir}/")
        print("Run `python -m benchmarks.generate_data` first.")
        return

    results = run_benchmarks(
        data_dir=data_dir,
        iterations=args.iterations,
        warmup=args.warmup,
        queries=args.queries,
    )

    print_summary(results)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        clean_results = {
            k: {key: val for key, val in v.items()
                 if key not in ("parquet", "iceberg") or not isinstance(val, dict)}
            | {fmt: {kk: vv for kk, vv in v[fmt].items() if kk != "raw_times"}
               for fmt in ("parquet", "iceberg")}
            for k, v in results.items()
        }
        with open(output_path, "w") as f:
            json.dump(clean_results, f, indent=2)
        print(f"Results saved to {output_path}")


if __name__ == "__main__":
    main()
