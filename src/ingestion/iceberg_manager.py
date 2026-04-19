"""Iceberg table management with DuckDB (using latest DuckDB v1.4.2+ approach)."""

import duckdb

from src.ingestion.config import get_config


class IcebergManager:
    """Manager for Apache Iceberg tables using DuckDB's native Iceberg extension.

    Based on DuckDB's latest Iceberg writes feature:
    https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """Initialize the Iceberg manager.

        Args:
            conn: DuckDB connection instance
        """
        self.conn = conn
        self.config = get_config()
        self.catalog_name = "iceberg_catalog"
        self.catalog_failed = False

        # Install and load Iceberg extension
        self._setup_duckdb_iceberg()

        # Attach to Iceberg catalog if using Iceberg
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

    def _attach_catalog(self):
        """Attach to an Iceberg catalog using DuckDB's ATTACH statement.

        Supports local filesystem, REST catalogs (Polaris, Lakekeeper), and cloud storage.
        """
        iceberg_config = self.config.get_iceberg_config()
        catalog_config = iceberg_config["catalog"]
        storage_backend = self.config.storage_backend
        warehouse_path = self.config.get_warehouse_path()

        try:
            catalog_type = catalog_config.get("type", "local")

            if catalog_type == "sql":
                # SQL Catalog (SQLite, PostgreSQL, etc.) - best for local dev
                sql_uri = catalog_config.get("sql_uri")
                if not sql_uri:
                    raise ValueError("SQL catalog requires 'sql_uri' in configuration")

                attach_sql = f"""
                    ATTACH '{warehouse_path}' AS {self.catalog_name} (
                        TYPE iceberg,
                        CATALOG_TYPE sql,
                        CATALOG_URI '{sql_uri}'
                    )
                """
                print(f"Using SQL catalog: {sql_uri}")

            elif catalog_type == "local":
                # Local filesystem - requires proper Iceberg metadata structure
                attach_sql = f"""
                    ATTACH '{warehouse_path}' AS {self.catalog_name} (
                        TYPE iceberg
                    )
                """
                print("Using local filesystem Iceberg catalog")

            elif catalog_type == "rest":
                # REST Catalog (Polaris, Nessie, Lakekeeper, etc.)
                rest_uri = catalog_config.get("uri")
                if not rest_uri:
                    raise ValueError("REST catalog requires 'uri' in configuration")

                # Load httpfs extension (required for REST catalogs)
                self.conn.execute("INSTALL httpfs")
                self.conn.execute("LOAD httpfs")

                # Check for Polaris authentication
                client_id = catalog_config.get("client_id")
                client_secret = catalog_config.get("client_secret")

                if client_id and client_secret:
                    # OAuth 2.0 authentication for Polaris
                    attach_sql = f"""
                        ATTACH '{warehouse_path}' AS {self.catalog_name} (
                            TYPE iceberg,
                            ENDPOINT '{rest_uri}',
                            AUTHORIZATION_TYPE 'oauth2',
                            CLIENT_ID '{client_id}',
                            CLIENT_SECRET '{client_secret}'
                        )
                    """
                    print(f"Using Polaris REST catalog at {rest_uri} with OAuth2 auth")
                else:
                    # No authentication
                    attach_sql = f"""
                        ATTACH '{warehouse_path}' AS {self.catalog_name} (
                            TYPE iceberg,
                            ENDPOINT '{rest_uri}',
                            AUTHORIZATION_TYPE 'none'
                        )
                    """
                    print(f"Using REST catalog at {rest_uri} (no authentication)")

            elif storage_backend == "s3":
                # S3-based warehouse with optional REST catalog
                storage_config = self.config.get_storage_config()
                s3_options = ["TYPE iceberg"]

                # Add REST catalog if configured
                if catalog_config.get("uri"):
                    s3_options.append("CATALOG_TYPE rest")
                    s3_options.append(f"URI '{catalog_config['uri']}'")

                # Add S3 configuration
                if storage_config.get("endpoint_url"):
                    s3_options.append(f"S3_ENDPOINT '{storage_config['endpoint_url']}'")
                if storage_config.get("region"):
                    s3_options.append(f"S3_REGION '{storage_config['region']}'")

                attach_sql = f"""
                    ATTACH '{warehouse_path}' AS {self.catalog_name} (
                        {", ".join(s3_options)}
                    )
                """
                print("Using S3-based Iceberg catalog")

            else:
                # Default: local filesystem
                attach_sql = f"""
                    ATTACH '{warehouse_path}' AS {self.catalog_name} (
                        TYPE iceberg
                    )
                """
                print("Using default local filesystem catalog")

            self.conn.execute(attach_sql)
            print(f"✓ Attached to Iceberg catalog: {self.catalog_name}")
            print(f"  Warehouse: {warehouse_path}")
            print(f"  Catalog type: {catalog_type}")

        except Exception as e:
            print(f"⚠ Warning: Could not attach to Iceberg catalog: {e}")
            print("  Falling back to native DuckDB tables")
            # Mark that catalog attachment failed
            self.catalog_failed = True

    def create_table(self, table_name: str, schema_sql: str, namespace: str = "default"):
        """Create an Iceberg table with the specified schema.

        Args:
            table_name: Name of the table to create
            schema_sql: SQL CREATE TABLE statement columns definition
            namespace: Iceberg namespace/database (default: 'default')

        Example:
            manager.create_table(
                "observations",
                '''
                observation_timestamp TIMESTAMPTZ NOT NULL,
                observation_id VARCHAR NOT NULL,
                temperature_degC DOUBLE,
                station_id VARCHAR
                '''
            )
        """
        if self.config.table_format != "iceberg":
            # Fallback to native DuckDB table
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql})")
            return

        full_table = f"{self.catalog_name}.{namespace}.{table_name}"

        try:
            # Create namespace if it doesn't exist
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{namespace}")

            # Create Iceberg table
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({schema_sql})")
            print(f"✓ Created Iceberg table: {full_table}")

        except Exception as e:
            print(f"⚠ Error creating Iceberg table: {e}")
            print("  Falling back to native DuckDB table")
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql})")

    def write_data(
        self, table_name: str, query: str, namespace: str = "default", mode: str = "append"
    ):
        """Write data to an Iceberg table using INSERT INTO.

        Args:
            table_name: Name of the table
            query: SQL query to generate the data
            namespace: Iceberg namespace (default: 'default')
            mode: 'append' or 'overwrite' (default: 'append')
        """
        if self.config.table_format != "iceberg":
            # Fallback to native DuckDB
            if mode == "overwrite":
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
            else:
                self.conn.execute(f"INSERT INTO {table_name} {query}")
            return

        full_table = f"{self.catalog_name}.{namespace}.{table_name}"

        try:
            # Use transaction for better performance (caches snapshot info)
            self.conn.execute("BEGIN TRANSACTION")

            if mode == "overwrite":
                # Delete existing data then insert
                self.conn.execute(f"DELETE FROM {full_table}")

            # Insert data
            self.conn.execute(f"INSERT INTO {full_table} {query}")

            self.conn.execute("COMMIT")
            print(f"✓ Data written to Iceberg table: {full_table} (mode: {mode})")

        except Exception as e:
            self.conn.execute("ROLLBACK")
            print(f"⚠ Error writing to Iceberg table: {e}")
            print("  Falling back to native DuckDB table")

            if mode == "overwrite":
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query}")
            else:
                self.conn.execute(f"INSERT INTO {table_name} {query}")

    def read_table(self, table_name: str, namespace: str = "default") -> str:
        """Get the fully qualified table identifier for reading.

        Args:
            table_name: Name of the table
            namespace: Iceberg namespace

        Returns:
            Fully qualified table identifier
        """
        # If catalog failed to attach or not using iceberg, use plain table name
        if self.config.table_format == "iceberg" and not self.catalog_failed:
            return f"{self.catalog_name}.{namespace}.{table_name}"
        else:
            return table_name

    def update_data(
        self, table_name: str, set_clause: str, where_clause: str, namespace: str = "default"
    ):
        """Update data in an Iceberg table (requires DuckDB v1.4.2+).

        Note: Updates are limited to non-partitioned, non-sorted tables.

        Args:
            table_name: Name of the table
            set_clause: SET clause (e.g., "temperature_degC = temperature_degC + 5")
            where_clause: WHERE clause (e.g., "station_id = 'XYZ'")
            namespace: Iceberg namespace
        """
        full_table = self.read_table(table_name, namespace)

        try:
            self.conn.execute(f"UPDATE {full_table} SET {set_clause} WHERE {where_clause}")
            print(f"✓ Updated records in: {full_table}")
        except Exception as e:
            print(f"⚠ Error updating table: {e}")

    def delete_data(self, table_name: str, where_clause: str, namespace: str = "default"):
        """Delete data from an Iceberg table.

        Args:
            table_name: Name of the table
            where_clause: WHERE clause (e.g., "observation_timestamp < '2024-01-01'")
            namespace: Iceberg namespace
        """
        full_table = self.read_table(table_name, namespace)

        try:
            self.conn.execute(f"DELETE FROM {full_table} WHERE {where_clause}")
            print(f"✓ Deleted records from: {full_table}")
        except Exception as e:
            print(f"⚠ Error deleting from table: {e}")


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with appropriate configuration for storage backend."""
    config = get_config()

    # Determine database path
    if config.storage_backend == "local":
        db_path = config.database_path
    else:
        # For cloud storage, use in-memory database
        db_path = ":memory:"

    conn = duckdb.connect(db_path)

    # Configure storage backend extensions
    storage_backend = config.storage_backend

    if storage_backend == "s3":
        # Install and configure AWS S3 extension
        storage_config = config.get_storage_config()
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        if region := storage_config.get("region"):
            conn.execute(f"SET s3_region='{region}'")
        if endpoint := storage_config.get("endpoint_url"):
            conn.execute(f"SET s3_endpoint='{endpoint}'")

        # AWS credentials are loaded from environment variables automatically
        print(f"✓ Configured S3 backend: {storage_config['bucket']}")

    elif storage_backend == "azure":
        # Install and configure Azure Blob Storage extension
        conn.execute("INSTALL azure")
        conn.execute("LOAD azure")
        # Azure credentials are loaded from environment variables automatically
        print("✓ Configured Azure Blob Storage backend")

    elif storage_backend == "gcs":
        # Install and configure Google Cloud Storage extension
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        # GCS credentials are loaded from GOOGLE_APPLICATION_CREDENTIALS env var
        print("✓ Configured Google Cloud Storage backend")

    return conn
