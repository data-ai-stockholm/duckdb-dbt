# Tests

Tests for the duckdb-dbt weather pipeline.

## Running Tests

```bash
make test

# Or directly
poetry run pytest tests/ -v

# With coverage
poetry run pytest tests/ --cov=src --cov-report=html
```

## Test Structure

```
tests/
├── __init__.py
└── test_config.py        # Config and project structure validation
```

## CI Integration

Tests run automatically on push and PR. See `.github/workflows/ci.yml`.