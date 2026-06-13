#!/usr/bin/env python3
"""
Iceberg REST Catalog Implementation.

Implements the Apache Iceberg REST Catalog specification:
https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
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
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize catalog with SQLite backend
warehouse_path = Path("warehouse").absolute()
catalog_db = warehouse_path / "catalog.db"
warehouse_path.mkdir(exist_ok=True)

catalog = SqlCatalog(
    "rest_catalog",
    **{"uri": f"sqlite:///{catalog_db}", "warehouse": f"file://{warehouse_path}"},
)
logger.info(f"Initialized Iceberg REST Catalog (DB: {catalog_db}, Warehouse: {warehouse_path})")


# ============================================================================
# CONFIGURATION ENDPOINTS
# ============================================================================


@app.route("/v1/config", methods=["GET"])
def get_config():
    """Get catalog configuration."""
    config = {
        "overrides": {"warehouse": str(warehouse_path)},
        "defaults": {"authorization_type": "none"},
    }
    logger.info(f"Config requested: {config}")
    return jsonify(config)


# ============================================================================
# NAMESPACE ENDPOINTS
# ============================================================================


@app.route("/v1/namespaces", methods=["GET"])
def list_namespaces():
    """List all namespaces."""
    try:
        namespaces = list(catalog.list_namespaces())
        logger.info(f"Listed {len(namespaces)} namespaces")
        return jsonify({"namespaces": [[ns] for ns in namespaces]})
    except Exception as e:
        logger.error(f"Error listing namespaces: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/v1/namespaces/<namespace>", methods=["GET"])
def get_namespace(namespace):
    """Get namespace properties."""
    try:
        props = catalog.load_namespace_properties(namespace)
        logger.info(f"Retrieved namespace: {namespace}")
        return jsonify({"namespace": [namespace], "properties": props})
    except Exception as e:
        logger.error(f"Error getting namespace {namespace}: {e}")
        return _error_response(f"Namespace not found: {namespace}", "NamespaceNotFoundException", 404)


@app.route("/v1/namespaces/<namespace>", methods=["POST"])
def create_namespace(namespace):
    """Create a new namespace."""
    try:
        properties = (request.get_json() or {}).get("properties", {})
        catalog.create_namespace(namespace, properties=properties)
        logger.info(f"Created namespace: {namespace}")
        return jsonify({"namespace": [namespace], "properties": properties}), 200
    except Exception as e:
        logger.error(f"Error creating namespace {namespace}: {e}")
        return _error_response(str(e), "AlreadyExistsException", 409)


@app.route("/v1/namespaces/<namespace>", methods=["DELETE"])
def drop_namespace(namespace):
    """Drop a namespace (must be empty)."""
    try:
        catalog.drop_namespace(namespace)
        logger.info(f"Dropped namespace: {namespace}")
        return "", 204
    except Exception as e:
        logger.error(f"Error dropping namespace {namespace}: {e}")
        return _error_response(str(e), "NamespaceNotEmptyException", 409)


# ============================================================================
# TABLE ENDPOINTS
# ============================================================================


@app.route("/v1/namespaces/<namespace>/tables", methods=["GET"])
def list_tables(namespace):
    """List all tables in a namespace."""
    try:
        tables = catalog.list_tables(namespace)
        response = {
            "identifiers": [
                {"namespace": [namespace], "name": str(t).split(".")[-1]} for t in tables
            ]
        }
        logger.info(f"Listed {len(tables)} tables in {namespace}")
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error listing tables in {namespace}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/v1/namespaces/<namespace>/tables", methods=["POST"])
def create_table(namespace):
    """Create a new Iceberg table."""
    try:
        data = request.get_json()
        table_name = data.get("name")
        logger.info(f"Creating table {namespace}.{table_name}")
        logger.debug(f"Schema: {json.dumps(data.get('schema'), indent=2)}")

        schema = _convert_schema(data.get("schema"))
        table = catalog.create_table(
            identifier=f"{namespace}.{table_name}",
            schema=schema,
            partition_spec=PartitionSpec(),
        )

        metadata_location = (
            f"{warehouse_path}/{namespace}/{table_name}/metadata/00000-{uuid.uuid4()}.metadata.json"
        )
        response = _build_table_response(table, str(metadata_location), include_snapshots=False)
        logger.info(f"✓ Created table {namespace}.{table_name}")
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error creating table {namespace}.{table_name}: {e}", exc_info=True)
        return _error_response(str(e), "Exception", 500)


@app.route("/v1/namespaces/<namespace>/tables/<table_name>", methods=["GET"])
def load_table(namespace, table_name):
    """Load table metadata."""
    try:
        table = catalog.load_table(f"{namespace}.{table_name}")
        metadata_location = f"{warehouse_path}/{namespace}/{table_name}/metadata/current.json"
        response = _build_table_response(table, metadata_location, include_snapshots=True)
        logger.info(f"Loaded table {namespace}.{table_name}")
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error loading table {namespace}.{table_name}: {e}")
        return _error_response(
            f"Table not found: {namespace}.{table_name}", "NoSuchTableException", 404
        )


@app.route("/v1/namespaces/<namespace>/tables/<table_name>", methods=["POST"])
def update_table(namespace, table_name):
    """Update table metadata."""
    try:
        data = request.get_json()
        logger.info(f"Updating table {namespace}.{table_name}")
        logger.debug(f"Update request: {json.dumps(data, indent=2)}")

        table = catalog.load_table(f"{namespace}.{table_name}")
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
    """Drop a table."""
    try:
        purge = request.args.get("purgeRequested", "false").lower() == "true"
        catalog.drop_table(f"{namespace}.{table_name}")
        logger.info(f"Dropped table {namespace}.{table_name} (purge={purge})")
        return "", 204
    except Exception as e:
        logger.error(f"Error dropping table {namespace}.{table_name}: {e}")
        return jsonify({"error": str(e)}), 404


# ============================================================================
# HELPERS
# ============================================================================

# Bidirectional type mapping: PyIceberg type class -> REST API string
_PRIMITIVE_TYPE_MAP = {
    StringType: "string",
    DoubleType: "double",
    FloatType: "float",
    IntegerType: "int",
    LongType: "long",
    BooleanType: "boolean",
    TimestamptzType: "timestamptz",
}

# Reverse map: REST string -> PyIceberg instance
_REST_TYPE_MAP = {v: k() for k, v in _PRIMITIVE_TYPE_MAP.items()}


def _error_response(message: str, error_type: str, code: int):
    return jsonify({"error": {"message": message, "type": error_type, "code": code}}), code


def _build_table_response(table, metadata_location: str, include_snapshots: bool) -> dict:
    """Build a standard table metadata response."""
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
        "last-partition-id": 0,
        "sort-orders": [],
        "default-sort-order-id": 0,
    }

    if include_snapshots:
        metadata["snapshots"] = [
            {
                "snapshot-id": snap.snapshot_id,
                "timestamp-ms": snap.timestamp_ms,
                "summary": snap.summary.dict() if snap.summary else {},
                "manifest-list": snap.manifest_list,
            }
            for snap in (table.metadata.snapshots or [])
        ]
        if table.metadata.current_snapshot_id is not None:
            metadata["current-snapshot-id"] = table.metadata.current_snapshot_id
    else:
        metadata.update({"snapshots": [], "snapshot-log": [], "metadata-log": []})

    return {"metadata-location": metadata_location, "metadata": metadata}


def _convert_schema(schema_data: dict) -> Schema:
    """Convert REST API schema format to PyIceberg Schema."""
    fields = [
        NestedField(
            field_id=f["id"],
            name=f["name"],
            field_type=_convert_type(f["type"]),
            required=f.get("required", False),
        )
        for f in schema_data.get("fields", [])
    ]
    return Schema(*fields, schema_id=schema_data.get("schema-id", 0))


def _convert_type(type_spec):
    """Convert REST API type to PyIceberg type."""
    if isinstance(type_spec, str):
        return _REST_TYPE_MAP.get(type_spec, StringType())

    if isinstance(type_spec, dict):
        if type_spec["type"] == "list":
            return ListType(
                element_id=type_spec.get("element-id", 1),
                element_type=_convert_type(type_spec["element"]),
                element_required=type_spec.get("element-required", False),
            )
        if type_spec["type"] == "struct":
            return StructType(*[
                NestedField(
                    field_id=f["id"], name=f["name"],
                    field_type=_convert_type(f["type"]), required=f.get("required", False),
                )
                for f in type_spec["fields"]
            ])

    return StringType()


def _type_to_dict(field_type):
    """Convert PyIceberg type to REST API format."""
    # Check primitive types
    for type_cls, type_str in _PRIMITIVE_TYPE_MAP.items():
        if isinstance(field_type, type_cls):
            return type_str

    # Complex types
    if isinstance(field_type, ListType):
        return {
            "type": "list",
            "element-id": field_type.element_id,
            "element": _type_to_dict(field_type.element_type),
            "element-required": field_type.element_required,
        }
    if isinstance(field_type, StructType):
        return {
            "type": "struct",
            "fields": [
                {"id": f.field_id, "name": f.name, "type": _type_to_dict(f.field_type), "required": f.required}
                for f in field_type.fields
            ],
        }
    if isinstance(field_type, MapType):
        return {
            "type": "map",
            "key-id": field_type.key_id,
            "key": _type_to_dict(field_type.key_type),
            "value-id": field_type.value_id,
            "value": _type_to_dict(field_type.value_type),
            "value-required": field_type.value_required,
        }

    return str(field_type)


def _schema_to_dict(schema: Schema) -> dict:
    """Convert PyIceberg Schema to REST API format."""
    return {
        "type": "struct",
        "schema-id": schema.schema_id,
        "fields": [
            {"id": f.field_id, "name": f.name, "type": _type_to_dict(f.field_type), "required": f.required}
            for f in schema.fields
        ],
    }


# ============================================================================
# HEALTH & MONITORING
# ============================================================================


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "catalog": "iceberg-rest", "warehouse": str(warehouse_path)})


@app.route("/metrics", methods=["GET"])
def metrics():
    try:
        namespaces = list(catalog.list_namespaces())
        total_tables = sum(len(list(catalog.list_tables(ns))) for ns in namespaces)
        return jsonify({"namespaces": len(namespaces), "tables": total_tables, "warehouse": str(warehouse_path)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================================
# STARTUP
# ============================================================================


def main():
    """Start the Iceberg REST Catalog server."""
    try:
        catalog.create_namespace("weather_data")
        logger.info("Created default namespace: weather_data")
    except Exception:
        logger.info("Namespace weather_data already exists")

    print(f"\n{'=' * 70}")
    print("ICEBERG REST CATALOG SERVER")
    print(f"{'=' * 70}")
    print(f"  Catalog DB:  {catalog_db}")
    print(f"  Warehouse:   {warehouse_path}")
    print(f"  REST API:    http://localhost:8181")
    print(f"  Health:      http://localhost:8181/health")
    print(f"{'=' * 70}\n")

    app.run(host="0.0.0.0", port=8181, debug=True)


if __name__ == "__main__":
    main()