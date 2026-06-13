"""Configuration management for the weather data pipeline."""

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Maps environment variables to their config key paths
_ENV_OVERRIDES = {
    "STORAGE_BACKEND": ["storage", "backend"],
    "DATABASE_PATH": ["storage", "local", "database_path"],
    "S3_BUCKET": ["storage", "s3", "bucket"],
    "AWS_DEFAULT_REGION": ["storage", "s3", "region"],
    "S3_ENDPOINT_URL": ["storage", "s3", "endpoint_url"],
    "ICEBERG_REST_URI": ["iceberg", "catalog", "uri"],
    "ICEBERG_WAREHOUSE": ["iceberg", "catalog", "warehouse"],
    "ICEBERG_CATALOG_TYPE": ["iceberg", "catalog", "type"],
    "ICEBERG_CLIENT_ID": ["iceberg", "catalog", "client_id"],
    "ICEBERG_CLIENT_SECRET": ["iceberg", "catalog", "client_secret"],
    "TABLE_FORMAT": ["table_format"],
}


class Config:
    """Configuration manager for the pipeline."""

    def __init__(self, config_path: str = "config/storage.yaml"):
        load_dotenv()

        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_file) as f:
            self._config = yaml.safe_load(f)

        # Apply env var overrides
        for env_var, keys in _ENV_OVERRIDES.items():
            if val := os.getenv(env_var):
                d = self._config
                for k in keys[:-1]:
                    d = d[k]
                d[keys[-1]] = val

    @property
    def storage_backend(self) -> str:
        return self._config["storage"]["backend"]

    @property
    def table_format(self) -> str:
        return self._config["table_format"]

    @property
    def database_path(self) -> str:
        return self._config["storage"]["local"]["database_path"]

    @property
    def data_dir(self) -> str:
        return self._config["storage"]["local"]["data_dir"]

    def get_storage_config(self) -> dict[str, Any]:
        return self._config["storage"][self.storage_backend]

    def get_iceberg_config(self) -> dict[str, Any]:
        return self._config["iceberg"]

    def get_warehouse_path(self) -> str:
        if self.storage_backend == "local":
            return self._config["iceberg"]["catalog"]["warehouse"]
        return self._config["storage"][self.storage_backend]["warehouse_path"]

    def get_api_config(self) -> dict[str, Any]:
        return self._config["api"]

    def get_processing_config(self) -> dict[str, Any]:
        return self._config["processing"]


# Global configuration instance
_config: Config | None = None


def get_config() -> Config:
    """Get or create the global configuration instance."""
    global _config
    if _config is None:
        _config = Config()
    return _config