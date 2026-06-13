"""Iceberg table management with DuckDB (using latest DuckDB v1.4.2+ approach)."""

import duckdb

from src.ingestion.config import get_config


class IcebergManager:
    """Manager for Apache Iceberg tables using DuckDB's native Iceberg extension."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.config = get_config()
        self.catalog_name = "iceberg_catalog"
        self.catalog_failed = False

        self._setup_duckdb_iceberg()
        if self.config.table_format == "iceberg":
            self._attach_catalog()

    def _setup_duckdb_iceberg(self):
        """Install and load DuckDB Iceberg extension."""
        try:
            self.conn.execute("INSTALL iceberg")
            self.conn.execute("LOAD iceberg")
            print("✓ DuckDB Iceberg extension loaded successfully")
        except Exception as e:
            print(f"⚠ Warning: Could not load Iceberg extension: {e}")
            print("  Continuing with native DuckDB tables")

    def _build_attach_sql(self, catalog_type, catalog_config, warehouse_path):
        """Build ATTACH SQL for the given catalog type."""
        if catalog_type == "sql":
            sql_uri = catalog_config.get("sql_uri")
            if not sql_uri:
                raise ValueError("SQL catalog requires 'sql_uri' in configuration")
            opts = f"TYPE iceberg, CATALOG_TYPE sql, CATALOG_URI '{sql_uri}'"

        elif catalog_type == "rest":
            rest_uri = catalog_config.get("uri")
            if not rest_uri:
                raise ValueError("REST catalog requires 'uri' in configuration")
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")

            client_id = catalog_config.get("client_id")
            client_secret = catalog_config.get("client_secret")
            if client_id and client_secret:
                opts = (f"TYPE iceberg, ENDPOINT '{rest_uri}', "
                        f"AUTHORIZATION_TYPE 'oauth2', "
                        f"CLIENT_ID '{client_id}', CLIENT_SECRET '{client_secret}'")
            else:
                opts = f"TYPE iceberg, ENDPOINT '{rest_uri}', AUTHORIZATION_TYPE 'none'"

        elif self.config.storage_backend == "s3":
            storage_config = self.config.get_storage_config()
            parts = ["TYPE iceberg"]
            if catalog_config.get("uri"):
                parts += ["CATALOG_TYPE rest", f"URI '{catalog_config['uri']}'"]
            if ep := storage_config.get("endpoint_url"):
                parts.append(f"S3_ENDPOINT '{ep}'")
            if region := storage_config.get("region"):
                parts.append(f"S3_REGION '{region}'")
            opts = ", ".join(parts)

        else:
            opts = "TYPE iceberg"

        return f"ATTACH '{warehouse_path}' AS {self.catalog_name} ({opts})"

    def _attach_catalog(self):
        """Attach to an Iceberg catalog."""
        iceberg_config = self.config.get_iceberg_config()
        catalog_config = iceberg_config["catalog"]
        warehouse_path = self.config.get_warehouse_path()
        catalog_type = catalog_config.get("type", "local")

        try:
            attach_sql = self._build_attach_sql(catalog_type, catalog_config, warehouse_path)
            self.conn.execute(attach_sql)
            print(f"✓ Attached to Iceberg catalog: {self.catalog_name}")
            print(f"  Warehouse: {warehouse_path}, Type: {catalog_type}")
        except Exception as e:
            print(f"⚠ Warning: Could not attach to Iceberg catalog: {e}")
            print("  Falling back to native DuckDB tables")
            self.catalog_failed = True

    def _full_table(self, table_name: str, namespace: str) -> str:
        return f"{self.catalog_name}.{namespace}.{table_name}"

    def _fallback_write(self, table_name: str, query: str, mode: str):
        if mode == "overwrite":
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
        else:
            self.conn.execute(f"INSERT INTO {table_name} {query}")

    def create_table(self, table_name: str, schema_sql: str, namespace: str = "default"):
        """Create an Iceberg table (or native DuckDB table as fallback)."""
        if self.config.table_format != "iceberg":
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql})")
            return

        full_table = self._full_table(table_name, namespace)
        try:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{namespace}")
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({schema_sql})")
            print(f"✓ Created Iceberg table: {full_table}")
        except Exception as e:
            print(f"⚠ Error creating Iceberg table: {e}, falling back to native DuckDB")
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql})")

    def write_data(self, table_name: str, query: str, namespace: str = "default", mode: str = "append"):
        """Write data to an Iceberg table using INSERT INTO."""
        if self.config.table_format != "iceberg":
            self._fallback_write(table_name, query, mode)
            return

        full_table = self._full_table(table_name, namespace)
        try:
            self.conn.execute("BEGIN TRANSACTION")
            if mode == "overwrite":
                self.conn.execute(f"DELETE FROM {full_table}")
            self.conn.execute(f"INSERT INTO {full_table} {query}")
            self.conn.execute("COMMIT")
            print(f"✓ Data written to Iceberg table: {full_table} (mode: {mode})")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            print(f"⚠ Error writing to Iceberg table: {e}, falling back to native DuckDB")
            self._fallback_write(table_name, query, mode)

    def read_table(self, table_name: str, namespace: str = "default") -> str:
        """Get the fully qualified table identifier for reading."""
        if self.config.table_format == "iceberg" and not self.catalog_failed:
            return self._full_table(table_name, namespace)
        return table_name

    def update_data(self, table_name: str, set_clause: str, where_clause: str, namespace: str = "default"):
        """Update data in an Iceberg table (requires DuckDB v1.4.2+)."""
        full_table = self.read_table(table_name, namespace)
        try:
            self.conn.execute(f"UPDATE {full_table} SET {set_clause} WHERE {where_clause}")
            print(f"✓ Updated records in: {full_table}")
        except Exception as e:
            print(f"⚠ Error updating table: {e}")

    def delete_data(self, table_name: str, where_clause: str, namespace: str = "default"):
        """Delete data from an Iceberg table."""
        full_table = self.read_table(table_name, namespace)
        try:
            self.conn.execute(f"DELETE FROM {full_table} WHERE {where_clause}")
            print(f"✓ Deleted records from: {full_table}")
        except Exception as e:
            print(f"⚠ Error deleting from table: {e}")


# Cloud extension configs: backend -> (extension, extra setup)
_CLOUD_EXTENSIONS = {
    "s3": ("httpfs", lambda conn, cfg: [
        conn.execute(f"SET s3_region='{r}'") if (r := cfg.get("region")) else None,
        conn.execute(f"SET s3_endpoint='{e}'") if (e := cfg.get("endpoint_url")) else None,
    ]),
    "azure": ("azure", lambda conn, cfg: None),
    "gcs": ("httpfs", lambda conn, cfg: None),
}


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with appropriate configuration for storage backend."""
    config = get_config()
    db_path = config.database_path if config.storage_backend == "local" else ":memory:"
    conn = duckdb.connect(db_path)

    backend = config.storage_backend
    if backend in _CLOUD_EXTENSIONS:
        ext, setup = _CLOUD_EXTENSIONS[backend]
        conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")
        setup(conn, config.get_storage_config())
        print(f"✓ Configured {backend} backend")

    return conn