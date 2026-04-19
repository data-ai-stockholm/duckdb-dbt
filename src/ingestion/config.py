"""Configuration management for the weather data pipeline."""

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


class Config:
    """Configuration manager for the pipeline."""

    def __init__(self, config_path: str = "config/storage.yaml"):
        """Initialize configuration from YAML file and environment variables."""
        # Load environment variables
        load_dotenv()

        # Load YAML configuration
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_file) as f:
            self._config = yaml.safe_load(f)

        # Override with environment variables if present
        self._apply_env_overrides()

    def _apply_env_overrides(self):
        """Apply environment variable overrides to configuration."""
        # Storage backend
        if backend := os.getenv("STORAGE_BACKEND"):
            self._config["storage"]["backend"] = backend

        # Database path
        if db_path := os.getenv("DATABASE_PATH"):
            self._config["storage"]["local"]["database_path"] = db_path

        # S3 configuration
        if s3_bucket := os.getenv("S3_BUCKET"):
            self._config["storage"]["s3"]["bucket"] = s3_bucket
        if s3_region := os.getenv("AWS_DEFAULT_REGION"):
            self._config["storage"]["s3"]["region"] = s3_region
        if s3_endpoint := os.getenv("S3_ENDPOINT_URL"):
            self._config["storage"]["s3"]["endpoint_url"] = s3_endpoint

        # Iceberg catalog configuration
        if iceberg_uri := os.getenv("ICEBERG_REST_URI"):
            self._config["iceberg"]["catalog"]["uri"] = iceberg_uri
        if warehouse := os.getenv("ICEBERG_WAREHOUSE"):
            self._config["iceberg"]["catalog"]["warehouse"] = warehouse

        # Iceberg Polaris authentication
        if catalog_type := os.getenv("ICEBERG_CATALOG_TYPE"):
            self._config["iceberg"]["catalog"]["type"] = catalog_type
        if client_id := os.getenv("ICEBERG_CLIENT_ID"):
            self._config["iceberg"]["catalog"]["client_id"] = client_id
        if client_secret := os.getenv("ICEBERG_CLIENT_SECRET"):
            self._config["iceberg"]["catalog"]["client_secret"] = client_secret

        # Table format
        if table_format := os.getenv("TABLE_FORMAT"):
            self._config["table_format"] = table_format

    @property
    def storage_backend(self) -> str:
        """Get the configured storage backend."""
        return self._config["storage"]["backend"]

    @property
    def table_format(self) -> str:
        """Get the configured table format (duckdb or iceberg)."""
        return self._config["table_format"]

    @property
    def database_path(self) -> str:
        """Get the database path for local storage."""
        return self._config["storage"]["local"]["database_path"]

    @property
    def data_dir(self) -> str:
        """Get the data directory for local storage."""
        return self._config["storage"]["local"]["data_dir"]

    def get_storage_config(self) -> dict[str, Any]:
        """Get storage configuration for the current backend."""
        backend = self.storage_backend
        return self._config["storage"][backend]

    def get_iceberg_config(self) -> dict[str, Any]:
        """Get Iceberg configuration."""
        return self._config["iceberg"]

    def get_warehouse_path(self) -> str:
        """Get the warehouse path based on storage backend."""
        backend = self.storage_backend

        if backend == "local":
            return self._config["iceberg"]["catalog"]["warehouse"]
        else:
            return self._config["storage"][backend]["warehouse_path"]

    def get_api_config(self) -> dict[str, Any]:
        """Get API configuration."""
        return self._config["api"]

    def get_processing_config(self) -> dict[str, Any]:
        """Get processing configuration."""
        return self._config["processing"]


# Global configuration instance
_config: Config | None = None


def get_config() -> Config:
    """Get or create the global configuration instance."""
    global _config
    if _config is None:
        _config = Config()
    return _config
