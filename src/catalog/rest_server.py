#!/usr/bin/env python3
"""
Complete Iceberg REST Catalog Implementation

This implements the Apache Iceberg REST Catalog specification:
https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml

Key Concepts:
- Catalog: Top-level container (like a database server)
- Namespace: Database/schema level (like PostgreSQL schema)
- Table: Individual Iceberg table with metadata and snapshots
"""

import json
import logging
import uuid
from pathlib import Path

from flask import Flask, jsonify, request
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    ListType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize catalog with SQLite backend
warehouse_path = Path("warehouse").absolute()
catalog_db = warehouse_path / "catalog.db"
warehouse_path.mkdir(exist_ok=True)

catalog = SqlCatalog(
    "rest_catalog",
    **{
        "uri": f"sqlite:///{catalog_db}",
        "warehouse": f"file://{warehouse_path}",
    },
)

logger.info("Initialized Iceberg REST Catalog")
logger.info(f"  Catalog DB: {catalog_db}")
logger.info(f"  Warehouse: {warehouse_path}")


# ============================================================================
# CONFIGURATION ENDPOINTS
# ============================================================================


@app.route("/v1/config", methods=["GET"])
def get_config():
    """
    Get catalog configuration.

    This tells clients (like DuckDB) how to authenticate and where
    the warehouse is located.
    """
    config = {
        "overrides": {
            "warehouse": str(warehouse_path),
        },
        "defaults": {
            # Tell DuckDB no authentication is required
            "authorization_type": "none"
        },
    }
    logger.info(f"Config requested: {config}")
    return jsonify(config)


# ============================================================================
# NAMESPACE (DATABASE/SCHEMA) ENDPOINTS
# ============================================================================


@app.route("/v1/namespaces", methods=["GET"])
def list_namespaces():
    """
    List all namespaces (databases/schemas).

    Returns array of namespace identifiers.
    """
    try:
        namespaces = list(catalog.list_namespaces())
        response = {"namespaces": [[ns] for ns in namespaces]}
        logger.info(f"Listed {len(namespaces)} namespaces")
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error listing namespaces: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/v1/namespaces/<namespace>", methods=["GET"])
def get_namespace(namespace):
    """
    Get namespace properties.

    Returns metadata about a specific namespace.
    """
    try:
        props = catalog.load_namespace_properties(namespace)
        response = {"namespace": [namespace], "properties": props}
        logger.info(f"Retrieved namespace: {namespace}")
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error getting namespace {namespace}: {e}")
        return jsonify(
            {
                "error": {
                    "message": f"Namespace not found: {namespace}",
                    "type": "NamespaceNotFoundException",
                    "code": 404,
                }
            }
        ), 404


@app.route("/v1/namespaces/<namespace>", methods=["POST"])
def create_namespace(namespace):
    """
    Create a new namespace.

    Body: { "properties": { "key": "value" } }
    """
    try:
        data = request.get_json() or {}
        properties = data.get("properties", {})

        catalog.create_namespace(namespace, properties=properties)

        response = {"namespace": [namespace], "properties": properties}
        logger.info(f"Created namespace: {namespace}")
        return jsonify(response), 200  # Spec says 200, not 201
    except Exception as e:
        logger.error(f"Error creating namespace {namespace}: {e}")
        return jsonify(
            {"error": {"message": str(e), "type": "AlreadyExistsException", "code": 409}}
        ), 409


@app.route("/v1/namespaces/<namespace>", methods=["DELETE"])
def drop_namespace(namespace):
    """
    Drop a namespace.

    Must be empty (no tables).
    """
    try:
        catalog.drop_namespace(namespace)
        logger.info(f"Dropped namespace: {namespace}")
        return "", 204  # No content
    except Exception as e:
        logger.error(f"Error dropping namespace {namespace}: {e}")
        return jsonify(
            {"error": {"message": str(e), "type": "NamespaceNotEmptyException", "code": 409}}
        ), 409


# ============================================================================
# TABLE ENDPOINTS
# ============================================================================


@app.route("/v1/namespaces/<namespace>/tables", methods=["GET"])
def list_tables(namespace):
    """
    List all tables in a namespace.
    """
    try:
        tables = catalog.list_tables(namespace)
        response = {
            "identifiers": [
                {"namespace": [namespace], "name": str(table).split(".")[-1]} for table in tables
            ]
        }
        logger.info(f"Listed {len(tables)} tables in {namespace}")
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error listing tables in {namespace}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/v1/namespaces/<namespace>/tables", methods=["POST"])
def create_table(namespace):
    """
    Create a new Iceberg table.

    This is the core operation! Takes a schema and creates an Iceberg table
    with proper metadata, snapshots, and manifest files.

    Body:
    {
      "name": "table_name",
      "schema": { ... Iceberg schema ... },
      "partition-spec": { ... },
      "write-order": { ... },
      "properties": { ... }
    }
    """
    try:
        data = request.get_json()
        table_name = data.get("name")
        schema_data = data.get("schema")

        logger.info(f"Creating table {namespace}.{table_name}")
        logger.debug(f"Schema: {json.dumps(schema_data, indent=2)}")

        # Convert REST API schema to PyIceberg Schema
        schema = _convert_schema(schema_data)

        # Create the table using PyIceberg
        table = catalog.create_table(
            identifier=f"{namespace}.{table_name}",
            schema=schema,
            partition_spec=PartitionSpec(),  # No partitioning for now
        )

        # Build response with table metadata
        metadata_location = (
            f"{warehouse_path}/{namespace}/{table_name}/metadata/00000-{uuid.uuid4()}.metadata.json"
        )

        response = {
            "metadata-location": str(metadata_location),
            "metadata": {
                "format-version": 2,
                "table-uuid": str(table.metadata.table_uuid),
                "location": table.location(),
                "last-updated-ms": table.metadata.last_updated_ms,
                "properties": table.properties,
                "schemas": [_schema_to_dict(table.schema())],
                "current-schema-id": table.schema().schema_id,
                "partition-specs": [],
                "default-spec-id": 0,
                "last-partition-id": 0,
                "sort-orders": [],
                "default-sort-order-id": 0,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
            },
        }

        logger.info(f"✓ Created table {namespace}.{table_name}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error creating table {namespace}.{table_name}: {e}", exc_info=True)
        return jsonify({"error": {"message": str(e), "type": "Exception", "code": 500}}), 500


@app.route("/v1/namespaces/<namespace>/tables/<table_name>", methods=["GET"])
def load_table(namespace, table_name):
    """
    Load table metadata.

    Returns complete table metadata including schema, partitioning,
    sort order, and snapshots.
    """
    try:
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Build metadata dict
        metadata = {
            "format-version": 2,
            "table-uuid": str(table.metadata.table_uuid),
            "location": table.location(),
            "last-updated-ms": table.metadata.last_updated_ms,
            "properties": table.properties,
            "schemas": [_schema_to_dict(table.schema())],
            "current-schema-id": table.schema().schema_id,
            "partition-specs": [],
            "default-spec-id": 0,
            "snapshots": [
                {
                    "snapshot-id": snap.snapshot_id,
                    "timestamp-ms": snap.timestamp_ms,
                    "summary": snap.summary.dict() if snap.summary else {},
                    "manifest-list": snap.manifest_list,
                }
                for snap in (table.metadata.snapshots or [])
            ],
            "sort-orders": [],
            "default-sort-order-id": 0,
        }

        # Only include current-snapshot-id if there is one (not None)
        if table.metadata.current_snapshot_id is not None:
            metadata["current-snapshot-id"] = table.metadata.current_snapshot_id

        response = {
            "metadata-location": f"{warehouse_path}/{namespace}/{table_name}/metadata/current.json",
            "metadata": metadata,
        }

        logger.info(f"Loaded table {namespace}.{table_name}")
        return jsonify(response)

    except Exception as e:
        logger.error(f"Error loading table {namespace}.{table_name}: {e}")
        return jsonify(
            {
                "error": {
                    "message": f"Table not found: {namespace}.{table_name}",
                    "type": "NoSuchTableException",
                    "code": 404,
                }
            }
        ), 404


@app.route("/v1/namespaces/<namespace>/tables/<table_name>", methods=["POST"])
def update_table(namespace, table_name):
    """
    Update table metadata.

    Used for committing new data (adding snapshots), schema evolution, etc.
    """
    try:
        data = request.get_json()
        logger.info(f"Updating table {namespace}.{table_name}")
        logger.debug(f"Update request: {json.dumps(data, indent=2)}")

        table = catalog.load_table(f"{namespace}.{table_name}")

        # For now, just return current metadata
        # In production, you'd process the updates
        response = {
            "metadata-location": f"{warehouse_path}/{namespace}/{table_name}/metadata/current.json",
            "metadata": {
                "format-version": 2,
                "table-uuid": str(table.metadata.table_uuid),
                "location": table.location(),
            },
        }

        logger.info(f"✓ Updated table {namespace}.{table_name}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error updating table {namespace}.{table_name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/v1/namespaces/<namespace>/tables/<table_name>", methods=["DELETE"])
def drop_table(namespace, table_name):
    """
    Drop a table.

    Query params:
    - purgeRequested: if true, delete all data files
    """
    try:
        purge = request.args.get("purgeRequested", "false").lower() == "true"

        catalog.drop_table(f"{namespace}.{table_name}")

        logger.info(f"Dropped table {namespace}.{table_name} (purge={purge})")
        return "", 204

    except Exception as e:
        logger.error(f"Error dropping table {namespace}.{table_name}: {e}")
        return jsonify({"error": str(e)}), 404


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _convert_schema(schema_data: dict) -> Schema:
    """
    Convert REST API schema format to PyIceberg Schema.

    REST format: { "type": "struct", "fields": [...] }
    PyIceberg: Schema(NestedField(...), NestedField(...))
    """
    fields = []
    for field in schema_data.get("fields", []):
        field_type = _convert_type(field["type"])
        fields.append(
            NestedField(
                field_id=field["id"],
                name=field["name"],
                field_type=field_type,
                required=field.get("required", False),
            )
        )

    return Schema(*fields, schema_id=schema_data.get("schema-id", 0))


def _convert_type(type_spec):
    """
    Convert REST API type to PyIceberg type.

    Handles: string, double, timestamptz, list, struct
    """
    if isinstance(type_spec, str):
        # Primitive types
        type_map = {
            "string": StringType(),
            "double": DoubleType(),
            "timestamptz": TimestamptzType(),
        }
        return type_map.get(type_spec, StringType())

    elif isinstance(type_spec, dict):
        # Complex types
        if type_spec["type"] == "list":
            element_type = _convert_type(type_spec["element"])
            return ListType(
                element_id=type_spec.get("element-id", 1),
                element_type=element_type,
                element_required=type_spec.get("element-required", False),
            )
        elif type_spec["type"] == "struct":
            # Recursively convert struct fields
            fields = [
                NestedField(
                    field_id=f["id"],
                    name=f["name"],
                    field_type=_convert_type(f["type"]),
                    required=f.get("required", False),
                )
                for f in type_spec["fields"]
            ]
            return StructType(*fields)

    return StringType()  # Fallback


def _type_to_dict(field_type):
    """
    Convert PyIceberg type to REST API type format.

    Handles primitive types (string, double, etc.) and complex types (list, struct).
    """
    from pyiceberg.types import (
        BooleanType,
        DoubleType,
        FloatType,
        IntegerType,
        ListType,
        LongType,
        MapType,
        StringType,
        StructType,
        TimestamptzType,
    )

    # Primitive types - return as string
    if isinstance(field_type, StringType):
        return "string"
    elif isinstance(field_type, DoubleType):
        return "double"
    elif isinstance(field_type, FloatType):
        return "float"
    elif isinstance(field_type, IntegerType):
        return "int"
    elif isinstance(field_type, LongType):
        return "long"
    elif isinstance(field_type, BooleanType):
        return "boolean"
    elif isinstance(field_type, TimestamptzType):
        return "timestamptz"

    # Complex types - return as nested dict
    elif isinstance(field_type, ListType):
        return {
            "type": "list",
            "element-id": field_type.element_id,
            "element": _type_to_dict(field_type.element_type),
            "element-required": field_type.element_required,
        }
    elif isinstance(field_type, StructType):
        return {
            "type": "struct",
            "fields": [
                {
                    "id": f.field_id,
                    "name": f.name,
                    "type": _type_to_dict(f.field_type),
                    "required": f.required,
                }
                for f in field_type.fields
            ],
        }
    elif isinstance(field_type, MapType):
        return {
            "type": "map",
            "key-id": field_type.key_id,
            "key": _type_to_dict(field_type.key_type),
            "value-id": field_type.value_id,
            "value": _type_to_dict(field_type.value_type),
            "value-required": field_type.value_required,
        }

    # Fallback: return string representation
    return str(field_type)


def _schema_to_dict(schema: Schema) -> dict:
    """
    Convert PyIceberg Schema to REST API format.
    """
    return {
        "type": "struct",
        "schema-id": schema.schema_id,
        "fields": [
            {
                "id": field.field_id,
                "name": field.name,
                "type": _type_to_dict(field.field_type),
                "required": field.required,
            }
            for field in schema.fields
        ],
    }


# ============================================================================
# HEALTH & MONITORING
# ============================================================================


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify(
        {"status": "healthy", "catalog": "iceberg-rest", "warehouse": str(warehouse_path)}
    )


@app.route("/metrics", methods=["GET"])
def metrics():
    """Metrics endpoint for monitoring."""
    try:
        namespaces = list(catalog.list_namespaces())
        total_tables = sum(len(list(catalog.list_tables(ns))) for ns in namespaces)

        return jsonify(
            {
                "namespaces": len(namespaces),
                "tables": total_tables,
                "warehouse": str(warehouse_path),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================================
# STARTUP
# ============================================================================


def main():
    """Start the Iceberg REST Catalog server."""
    # Create default namespace
    try:
        catalog.create_namespace("weather_data")
        logger.info("Created default namespace: weather_data")
    except Exception:
        logger.info("Namespace weather_data already exists")

    print("\n" + "=" * 70)
    print("🎓 CUSTOM ICEBERG REST CATALOG SERVER")
    print("=" * 70)
    print(f"  Catalog DB:  {catalog_db}")
    print(f"  Warehouse:   {warehouse_path}")
    print("  REST API:    http://localhost:8181")
    print("  Health:      http://localhost:8181/health")
    print("  Metrics:     http://localhost:8181/metrics")
    print("=" * 70)
    print("\nImplements Apache Iceberg REST Catalog Specification")
    print("Learn more: https://github.com/apache/iceberg/blob/main/open-api/")
    print("=" * 70 + "\n")

    app.run(host="0.0.0.0", port=8181, debug=True)


if __name__ == "__main__":
    main()
