# Tests

This directory contains tests for the duckdb-dbt weather pipeline.

## Running Tests

```bash
# Run all tests
make test

# Run with coverage
poetry run pytest tests/ --cov=src --cov-report=html

# Run specific test file
poetry run pytest tests/test_catalog.py -v
```

## Test Structure

```
tests/
├── test_catalog.py       # REST catalog tests
├── test_ingestion.py     # Data ingestion tests
├── test_flows.py         # Prefect flow tests
└── fixtures/             # Test data and fixtures
```

## Writing Tests

### Example Test

```python
import pytest
from src.ingestion.config import Config

def test_config_loads_storage_yaml():
    """Test that config correctly loads storage.yaml"""
    config = Config()
    assert config.storage_backend == "local"
    assert config.table_format == "iceberg"
```

### Testing Prefect Flows

```python
from prefect.testing.utilities import prefect_test_harness

def test_weather_ingestion_flow():
    """Test weather ingestion flow execution"""
    with prefect_test_harness():
        from src.flows.weather_ingestion import weather_ingestion_flow
        result = weather_ingestion_flow()
        assert result["status"] == "success"
```

## Coverage Goals

- **Minimum**: 70% coverage
- **Target**: 80%+ coverage
- **Critical paths**: 90%+ coverage (catalog, ingestion)

## CI Integration

Tests run automatically on:
- Every push to main
- Every pull request
- Pre-commit hooks (optional)

See `.github/workflows/ci.yml` for CI configuration.
