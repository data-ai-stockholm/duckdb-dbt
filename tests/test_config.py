"""Basic configuration tests."""

import pytest
from pathlib import Path


def test_config_file_exists():
    """Verify storage config file exists."""
    config_path = Path("config/storage.yaml")
    assert config_path.exists(), "config/storage.yaml should exist"


def test_dbt_project_exists():
    """Verify dbt project file exists."""
    dbt_project = Path("dbt/dbt_project.yml")
    assert dbt_project.exists(), "dbt/dbt_project.yml should exist"


def test_dbt_profiles_exists():
    """Verify dbt profiles file exists."""
    profiles = Path("dbt/profiles.yml")
    assert profiles.exists(), "dbt/profiles.yml should exist"
