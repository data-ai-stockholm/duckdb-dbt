#!/usr/bin/env python3
"""Quick script to query Iceberg tables locally."""

from pyiceberg.catalog.sql import SqlCatalog

# Connect to catalog
catalog = SqlCatalog(
    "default",
    **{
        "uri": "sqlite:///warehouse/catalog.db",
        "warehouse": "file://warehouse/",
    }
)

# List namespaces
print("\n=== Namespaces ===")
namespaces = catalog.list_namespaces()
print(f"  {namespaces}")

# List all tables
print("\n=== Available Tables ===")
for namespace_tuple in namespaces:
    namespace = namespace_tuple[0] if isinstance(namespace_tuple, tuple) else namespace_tuple
    try:
        tables = catalog.list_tables(namespace)
        for table_id in tables:
            print(f"  {table_id}")
    except Exception as e:
        print(f"  Error listing {namespace}: {e}")

# Query test data
print("\n=== Test Weather Data (Sample) ===")
try:
    table = catalog.load_table("test.weather")
    df = table.scan().to_pandas().head(10)
    print(df)
    print(f"\nTotal records: {len(table.scan().to_pandas())}")
except Exception as e:
    print(f"Could not load test.weather: {e}")

# Try weather_data.observations
print("\n=== Weather Observations (Sample) ===")
try:
    table = catalog.load_table("weather_data.observations")
    df = table.scan().to_pandas().head(10)
    print(df)
    print(f"\nTotal records: {len(table.scan().to_pandas())}")
except Exception as e:
    print(f"Could not load weather_data.observations: {e}")
