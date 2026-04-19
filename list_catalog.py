#!/usr/bin/env python3
"""List all catalogs, namespaces, and tables in the Iceberg warehouse."""

import os
from pathlib import Path

warehouse = Path("warehouse")

print("=" * 70)
print("ICEBERG CATALOG STRUCTURE")
print("=" * 70)

# 1. Catalog
print("\n📦 CATALOG")
print(f"  Location: {warehouse.absolute()}")
catalog_db = warehouse / "catalog.db"
print(f"  Metadata: {catalog_db} ({'exists' if catalog_db.exists() else 'missing'})")

# 2. Namespaces (directories in warehouse)
print("\n📂 NAMESPACES")
namespaces = []
for item in warehouse.iterdir():
    if item.is_dir() and not item.name.startswith('.'):
        namespaces.append(item.name)
        print(f"  ├─ {item.name}/")

if not namespaces:
    print("  (none found)")

# 3. Tables (subdirectories in namespaces)
print("\n📊 TABLES")
for namespace in sorted(namespaces):
    namespace_path = warehouse / namespace
    tables = []

    for item in namespace_path.iterdir():
        if item.is_dir() and not item.name.startswith('.'):
            # Check if it has metadata directory (Iceberg table marker)
            if (item / "metadata").exists():
                tables.append(item.name)

    if tables:
        print(f"  {namespace}/")
        for table in sorted(tables):
            table_path = namespace_path / table
            metadata_dir = table_path / "metadata"
            data_dir = table_path / "data"

            # Count files
            metadata_files = len(list(metadata_dir.glob("*.json"))) if metadata_dir.exists() else 0
            data_files = len(list(data_dir.glob("*.parquet"))) if data_dir.exists() else 0

            print(f"    ├─ {table}")
            print(f"    │    Metadata: {metadata_files} files")
            print(f"    │    Data: {data_files} parquet files")
    else:
        print(f"  {namespace}/ (no tables)")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"  Namespaces: {len(namespaces)}")

total_tables = 0
for namespace in namespaces:
    namespace_path = warehouse / namespace
    for item in namespace_path.iterdir():
        if item.is_dir() and (item / "metadata").exists():
            total_tables += 1

print(f"  Tables: {total_tables}")
print("=" * 70)
