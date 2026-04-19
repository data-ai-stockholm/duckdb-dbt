# Technical Documentation - Apache Iceberg REST Catalog Implementation

## Overview

This document provides a deep technical dive into the custom Apache Iceberg REST Catalog implementation, covering architecture, implementation details, and lessons learned.

## Table of Contents

1. [Repository Structure](#repository-structure)
2. [Iceberg Architecture](#iceberg-architecture)
3. [REST Catalog Implementation](#rest-catalog-implementation)
4. [Type System & Schema Conversion](#type-system--schema-conversion)
5. [DuckDB Integration](#duckdb-integration)
6. [Troubleshooting](#troubleshooting)
7. [Production Considerations](#production-considerations)
8. [Prefect Orchestration & Deployment](#prefect-orchestration--deployment)
9. [Makefile Command Reference](#makefile-command-reference)

---

## Repository Structure

The repository follows a modular structure with clear separation of concerns:

```
duckdb-dbt/
├── .github/workflows/           # GitHub Actions CI/CD
│   ├── ci.yml                   # Main CI: linting, testing, type checking
│   └── dbt.yml                  # dbt validation workflow
│
├── config/                      # Application configuration
│   └── storage.yaml            # Storage backend config (local/s3/azure/gcs)
│
├── dbt/                         # dbt project (all dbt files)
│   ├── dbt_project.yml         # dbt project configuration
│   ├── profiles.yml            # dbt connection profiles
│   ├── .user.yml               # User-specific settings (gitignored)
│   └── models/                 # dbt data models
│       ├── staging/            # Staging layer (stg_observations)
│       └── marts/              # Marts layer (fact/dim tables)
│
├── docs/                        # Documentation
│   ├── README.md               # User guide and quick start
│   └── CLAUDE.md               # Technical deep-dive (this file)
│
├── scripts/                     # Utility scripts
│   ├── deploy_flows.py         # Prefect deployment script
│   └── run_with_ui.sh          # Helper script for UI-based workflows
│
├── src/                         # Source code
│   ├── catalog/                # Iceberg REST catalog implementation
│   │   └── rest_server.py      # Flask REST API server
│   ├── ingestion/              # Data ingestion modules
│   │   ├── config.py           # Configuration manager
│   │   ├── iceberg_manager.py  # DuckDB + Iceberg integration
│   │   ├── fetch_stations.py   # Weather station metadata fetcher
│   │   ├── fetch_observations.py # Weather data fetcher
│   │   └── write_observations.py # Data loader to Iceberg
│   └── flows/                  # Prefect orchestration flows
│       ├── demo_flow.py        # Demo flow showing Prefect features
│       ├── weather_ingestion.py # Weather data ingestion flow
│       ├── dbt_transformations.py # dbt transformation flow
│       └── main_pipeline.py    # Complete end-to-end pipeline
│
├── examples/
│   └── test_catalog.py         # Demo script with synthetic data
│
├── prefect.yaml                 # Prefect deployment configuration
├── pyproject.toml               # Poetry dependencies and scripts
├── Makefile                     # Build automation and commands
└── .gitignore                   # Git ignore patterns

# Generated/Ignored directories:
├── warehouse/                   # Iceberg data lake (gitignored)
├── ingestion_data/              # Downloaded weather data (gitignored)
├── target/                      # dbt artifacts (gitignored)
├── logs/                        # Application logs (gitignored)
└── .venv/                       # Python virtual environment (gitignored)
```

### Key Design Decisions

**Why this structure?**

1. **config/** - Centralizes all application configuration
   - `storage.yaml` contains storage backend settings
   - Separates config from code

2. **dbt/** - All dbt-related files in one place
   - Makes dbt commands cleaner: `dbt run --project-dir dbt`
   - Easier to understand what's dbt vs. application code
   - `.user.yml` properly scoped to dbt

3. **docs/** - Documentation separate from code
   - README.md for users (quick start, commands)
   - CLAUDE.md for technical deep-dive
   - Keeps root directory clean

4. **scripts/** - Utility scripts separate from source code
   - `deploy_flows.py` for Prefect deployments
   - `run_with_ui.sh` for helper scripts
   - Clear distinction from application code

5. **src/** - All application source code
   - `catalog/` - REST catalog implementation
   - `ingestion/` - Data fetching and loading
   - `flows/` - Prefect workflow definitions

### File Locations Quick Reference

| What you need | Where to find it |
|---------------|------------------|
| Storage config | `config/storage.yaml` |
| dbt models | `dbt/models/` |
| dbt configuration | `dbt/dbt_project.yml`, `dbt/profiles.yml` |
| REST catalog server | `src/catalog/rest_server.py` |
| Data ingestion | `src/ingestion/` |
| Prefect flows | `src/flows/` |
| Deploy to Prefect | `scripts/deploy_flows.py` |
| Quick demo | `examples/test_catalog.py` |
| Documentation | `docs/README.md`, `docs/CLAUDE.md` |

---

## Iceberg Architecture

### Three-Level Hierarchy

```
Catalog
  └─ Namespace (database/schema)
       └─ Table
            ├─ Metadata (JSON files with versioning)
            ├─ Snapshots (point-in-time table state)
            ├─ Manifests (lists of data files)
            └─ Data Files (Parquet/ORC/Avro)
```

### Metadata Evolution

Iceberg maintains a complete history of all table changes:

```
warehouse/weather_data.db/observations/metadata/
├── 00000-{uuid}.metadata.json  # Initial table creation
├── 00001-{uuid}.metadata.json  # After first write
├── 00002-{uuid}.metadata.json  # After second write
├── snap-{id}-0-{uuid}.avro      # Snapshot manifest
└── {uuid}-m0.avro               # Data file manifest
```

Each metadata file contains:
- **table-uuid**: Unique identifier for the table
- **schemas**: Array of schema versions
- **current-schema-id**: Active schema
- **partition-specs**: Partitioning configuration
- **sort-orders**: Sort order specifications
- **snapshots**: Array of table snapshots
- **current-snapshot-id**: Latest snapshot (omit if none)

### Snapshot Structure

```json
{
  "snapshot-id": 8234101027069594364,
  "timestamp-ms": 1764964231317,
  "summary": {
    "operation": "append",
    "added-records": "5",
    "total-records": "5",
    "added-data-files": "1"
  },
  "manifest-list": "file://.../snap-*.avro"
}
```

---

## REST Catalog Implementation

### REST API Endpoints

Location: `src/catalog/rest_server.py`

#### Configuration

```python
@app.route('/v1/config', methods=['GET'])
def get_config():
    return jsonify({
        "defaults": {"authorization_type": "none"}
    })
```

**Critical**: DuckDB defaults to OAuth2, must explicitly set `authorization_type: none`

#### Namespace Operations

```python
GET    /v1/namespaces              # List all namespaces
GET    /v1/namespaces/{ns}         # Get namespace properties
POST   /v1/namespaces/{ns}         # Create namespace
DELETE /v1/namespaces/{ns}         # Drop namespace (must be empty)
```

#### Table Operations

```python
GET    /v1/namespaces/{ns}/tables           # List tables
POST   /v1/namespaces/{ns}/tables           # Create table
GET    /v1/namespaces/{ns}/tables/{table}   # Load table metadata
POST   /v1/namespaces/{ns}/tables/{table}   # Update table
DELETE /v1/namespaces/{ns}/tables/{table}   # Drop table
```

### Schema Conversion

The hardest part of implementing a REST catalog is converting between REST API format and PyIceberg internal representation.

#### REST → PyIceberg

```python
def _convert_schema(schema_data: dict) -> Schema:
    """Convert REST API schema to PyIceberg Schema."""
    fields = []
    for field in schema_data.get('fields', []):
        field_type = _convert_type(field['type'])
        fields.append(NestedField(
            field_id=field['id'],
            name=field['name'],
            field_type=field_type,
            required=field.get('required', False),
        ))
    return Schema(*fields, schema_id=schema_data.get('schema-id', 0))
```

#### PyIceberg → REST

```python
def _schema_to_dict(schema: Schema) -> dict:
    """Convert PyIceberg Schema to REST API format."""
    return {
        "type": "struct",
        "schema-id": schema.schema_id,
        "fields": [
            {
                "id": field.field_id,
                "name": field.name,
                "type": _type_to_dict(field.field_type),  # Recursive!
                "required": field.required,
            }
            for field in schema.fields
        ]
    }
```

---

## Type System & Schema Conversion

### Primitive Types

```python
type_map = {
    'string': StringType(),
    'int': IntegerType(),
    'long': LongType(),
    'float': FloatType(),
    'double': DoubleType(),
    'boolean': BooleanType(),
    'timestamptz': TimestamptzType(),
}
```

### Complex Types

#### List Type

REST API format:
```json
{
  "type": "list",
  "element-id": 6,
  "element": "string",
  "element-required": false
}
```

PyIceberg format:
```python
ListType(
    element_id=6,
    element_type=StringType(),
    element_required=False
)
```

Conversion logic:
```python
def _type_to_dict(field_type):
    if isinstance(field_type, ListType):
        return {
            "type": "list",
            "element-id": field_type.element_id,
            "element": _type_to_dict(field_type.element_type),  # Recursive!
            "element-required": field_type.element_required
        }
```

#### Struct Type

REST API format:
```json
{
  "type": "struct",
  "fields": [
    {"id": 1, "name": "city", "type": "string", "required": true},
    {"id": 2, "name": "temp", "type": "double", "required": false}
  ]
}
```

PyIceberg format:
```python
StructType(
    NestedField(1, "city", StringType(), required=True),
    NestedField(2, "temp", DoubleType(), required=False)
)
```

#### Map Type

```python
{
  "type": "map",
  "key-id": 10,
  "key": "string",
  "value-id": 11,
  "value": "int",
  "value-required": false
}
```

### Critical Issues Fixed

#### Issue 1: `current-snapshot-id` Must Be Omitted, Not Null

**Wrong:**
```python
metadata["current-snapshot-id"] = None  # DuckDB rejects this!
```

**Correct:**
```python
if table.metadata.current_snapshot_id is not None:
    metadata["current-snapshot-id"] = table.metadata.current_snapshot_id
```

**Why**: DuckDB validates types strictly. `null` is not a valid integer, so the field must be omitted entirely.

#### Issue 2: Type Serialization Requires Recursion

**Wrong:**
```python
"type": str(field.field_type)  # Produces "list<string>"
```

**Correct:**
```python
"type": _type_to_dict(field.field_type)  # Produces nested dict
```

**Why**: DuckDB needs the structured format to properly understand complex types.

---

## DuckDB Integration

Location: `src/ingestion/iceberg_manager.py`

### ATTACH Syntax

```python
# Correct DuckDB v1.4.2+ syntax
ATTACH 'warehouse/' AS iceberg_catalog (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'  # Critical!
)
```

**Common mistakes:**
- Using `URI` instead of `ENDPOINT`
- Forgetting `AUTHORIZATION_TYPE 'none'`
- Trying to use SQL catalog (not supported by DuckDB ATTACH)

### Writing Data

DuckDB's Iceberg write support is experimental (v1.4.2). Prefer PyIceberg for stability:

```python
# Using PyIceberg (stable)
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(...)
table = catalog.load_table("namespace.table")

# Convert pandas to PyArrow with correct nullable flags
arrow_table = pa.Table.from_pandas(df, schema=pa.schema([
    pa.field("id", pa.int32(), nullable=False),  # Required
    pa.field("name", pa.string(), nullable=True),  # Optional
]))

table.append(arrow_table)
```

**Why PyIceberg over DuckDB writes:**
- More stable (DuckDB Iceberg writes cause segfaults in some cases)
- Better error messages
- Proper validation

### Multi-Cloud Configuration

```python
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    storage_backend = config.storage_backend

    if storage_backend == "s3":
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute(f"SET s3_region='{region}'")

    elif storage_backend == "azure":
        conn.execute("INSTALL azure")
        conn.execute("LOAD azure")

    elif storage_backend == "gcs":
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

    return conn
```

---

## Troubleshooting

### REST Catalog Won't Start

**Symptoms:**
```
Address already in use: 8181
```

**Solution:**
```bash
# Find and kill process on port 8181
lsof -ti:8181 | xargs kill -9

# Restart
poetry run catalog-server
```

### OAuth2 Authentication Errors

**Symptoms:**
```
AUTHORIZATION_TYPE is 'oauth2', yet no 'secret' was provided
```

**Solution:**
Add to ATTACH statement:
```sql
ATTACH '...' AS cat (..., AUTHORIZATION_TYPE 'none')
```

Also ensure `/v1/config` endpoint returns:
```json
{"defaults": {"authorization_type": "none"}}
```

### DuckDB Segfault on Write

**Symptoms:**
```
Exit code 139 (segmentation fault)
```

**Solution:**
Use PyIceberg for writes instead of DuckDB:
```python
# Instead of:
conn.execute(f"INSERT INTO iceberg_table ...")

# Use:
table = catalog.load_table("namespace.table")
table.append(arrow_data)
```

### Schema Mismatch Errors

**Symptoms:**
```
Mismatch in fields:
❌ │ 1: id: required int  │ 1: id: optional int
```

**Solution:**
Ensure PyArrow schema matches Iceberg schema:
```python
arrow_table = pa.Table.from_pandas(df, schema=pa.schema([
    pa.field("id", pa.int32(), nullable=False),  # Match required=True
]))
```

### Empty Parquet Files

**Symptoms:**
```
File too small to be a Parquet file (107 bytes)
```

**Solution:**
API data fetching failed. Use synthetic data for testing:
```bash
poetry run python examples/test_catalog.py
```

---

## Production Considerations

### Authentication & Authorization

Current implementation uses `authorization_type: none`. For production:

```python
from flask import request
import jwt

@app.before_request
def authenticate():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, secret_key, algorithms=['HS256'])
        g.user = payload['user']
    except:
        return jsonify({"error": "Unauthorized"}), 401
```

### High Availability

**Replace SQLite with PostgreSQL:**

```python
catalog = SqlCatalog(
    "prod_catalog",
    **{
        "uri": "postgresql://user:pass@host:5432/iceberg",
        "warehouse": "s3://bucket/warehouse/",
    }
)
```

**Add connection pooling:**
```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    db_uri,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20
)
```

### Caching

Add Redis for metadata caching:

```python
import redis

cache = redis.Redis(host='localhost', port=6379)

@app.route('/v1/namespaces/<namespace>/tables/<table_name>')
def load_table(namespace, table_name):
    cache_key = f"table:{namespace}.{table_name}"

    # Check cache
    cached = cache.get(cache_key)
    if cached:
        return jsonify(json.loads(cached))

    # Load from catalog
    table = catalog.load_table(f"{namespace}.{table_name}")
    response = build_response(table)

    # Cache for 5 minutes
    cache.setex(cache_key, 300, json.dumps(response))

    return jsonify(response)
```

### Monitoring

Add Prometheus metrics:

```python
from prometheus_client import Counter, Histogram

request_count = Counter('catalog_requests_total', 'Total requests', ['method', 'endpoint'])
request_duration = Histogram('catalog_request_duration_seconds', 'Request duration')

@app.before_request
def before_request():
    g.start_time = time.time()

@app.after_request
def after_request(response):
    duration = time.time() - g.start_time
    request_count.labels(request.method, request.endpoint).inc()
    request_duration.observe(duration)
    return response
```

### Cloud Storage

**S3 Configuration:**

```yaml
storage:
  backend: s3
  s3:
    bucket: my-iceberg-data
    region: us-east-1
    warehouse_path: s3://my-iceberg-data/warehouse/
```

**Credentials:** Use IAM roles instead of access keys:

```python
# DuckDB automatically uses AWS credentials from:
# - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
# - ~/.aws/credentials
# - EC2 instance profile (in production)
```

---

## Performance Optimizations

### 1. Partition Pruning

```python
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

spec = PartitionSpec(
    PartitionField(
        source_id=2,  # timestamp column
        field_id=1000,
        name="day",
        transform=DayTransform()
    )
)

table = catalog.create_table(
    identifier="namespace.table",
    schema=schema,
    partition_spec=spec
)
```

### 2. File Compaction

```python
from pyiceberg.table import Table

def compact_table(table: Table):
    """Compact small files into larger ones."""
    # Get all data files
    snapshot = table.current_snapshot()
    manifests = snapshot.manifests()

    # Group small files
    small_files = [f for f in files if f.file_size_in_bytes < 128 * 1024 * 1024]

    # Rewrite as larger files
    # (Implementation depends on compute engine)
```

### 3. Snapshot Expiration

```python
from datetime import datetime, timedelta

def expire_snapshots(table: Table, older_than_days: int = 7):
    """Remove old snapshots to save storage."""
    cutoff = datetime.now() - timedelta(days=older_than_days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    table.expire_snapshots(older_than_ms=cutoff_ms)
```

---

## Advanced Features

### Schema Evolution

```python
from pyiceberg.table import Table

table = catalog.load_table("namespace.table")

# Add column
table.update_schema().add_column("humidity", DoubleType()).commit()

# Rename column
table.update_schema().rename_column("temp", "temperature").commit()

# Delete column
table.update_schema().delete_column("old_field").commit()
```

### Time Travel

```python
# Query specific snapshot
snapshot_id = table.metadata.snapshots[0].snapshot_id
df = table.scan(snapshot_id=snapshot_id).to_pandas()

# Query as of timestamp
as_of_timestamp_ms = int(datetime(2024, 1, 1).timestamp() * 1000)
df = table.scan(as_of_timestamp_ms=as_of_timestamp_ms).to_pandas()
```

### Branching (Iceberg v2)

```python
# Create branch for experimentation
table.manage_snapshots().create_branch("experiment").commit()

# Switch to branch
table.manage_snapshots().set_ref_snapshot("experiment", snapshot_id).commit()

# Merge branch
table.manage_snapshots().fast_forward_branch("main", "experiment").commit()
```

---

## File Locations

### REST Catalog Server
`src/catalog/rest_server.py` - Complete Flask implementation of Iceberg REST API

### DuckDB Integration
`src/ingestion/iceberg_manager.py` - ATTACH catalog, create/write tables

### Configuration
`src/ingestion/config.py` - Multi-backend config management

### Example
`examples/test_catalog.py` - Working demo with synthetic data

---

## References

- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [DuckDB Iceberg Writes](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb)

---

## Prefect Orchestration & Deployment

### Local Development

#### Install Prefect
```bash
poetry add prefect
poetry install
```

#### Start Prefect Server (Optional - for local UI)
```bash
prefect server start
```

Access the UI at http://localhost:4200

#### Run Flows Locally

```bash
# Weather ingestion only
poetry run python src/flows/weather_ingestion.py

# dbt transformations only
poetry run python src/flows/dbt_transformations.py

# Full pipeline
poetry run python src/flows/main_pipeline.py
```

### Prefect Cloud Setup

#### 1. Create Prefect Cloud Account
- Sign up at https://app.prefect.cloud
- Create a workspace

#### 2. Authenticate
```bash
prefect cloud login
```

#### 3. Create Work Pool
```bash
prefect work-pool create default-agent-pool --type process
```

#### 4. Deploy Flows
```bash
# Deploy all flows defined in prefect.yaml
prefect deploy --all

# Or deploy specific flow
prefect deploy -n weather-ingestion-hourly
```

#### 5. Start Worker
```bash
prefect worker start --pool default-agent-pool
```

### Deployment Schedules

The `prefect.yaml` defines three deployments:

| Deployment | Schedule | Description |
|------------|----------|-------------|
| `weather-ingestion-hourly` | Every hour | Fetches and loads weather data |
| `dbt-transformations-6hourly` | Every 6 hours | Runs dbt models and tests |
| `weather-pipeline-daily` | Daily at 6 AM | Complete end-to-end pipeline |

### Customizing Schedules

Edit `prefect.yaml` and modify the cron expressions:

```yaml
schedules:
  - cron: "0 */6 * * *"  # Every 6 hours
    timezone: "America/New_York"
```

Common cron patterns:
- `"0 * * * *"` - Every hour
- `"0 */6 * * *"` - Every 6 hours
- `"0 6 * * *"` - Daily at 6 AM
- `"0 6 * * 1"` - Weekly on Monday at 6 AM

### GitHub Actions CI/CD

#### CI Workflow

The CI workflow (`.github/workflows/ci.yml`) runs on every push and PR:

**Steps**:
1. ✅ Lint code with Ruff
2. ✅ Format check with Ruff
3. ✅ Type checking with mypy
4. ✅ Run tests with pytest
5. ✅ Upload coverage to Codecov

#### dbt CI Workflow

The dbt workflow (`.github/workflows/dbt.yml`) runs when dbt files change:

**Steps**:
1. ✅ dbt debug - Check connection
2. ✅ dbt deps - Install packages
3. ✅ dbt compile - Compile models
4. ✅ dbt parse - Validate syntax

#### Secrets Configuration

Add these secrets to your GitHub repository (Settings → Secrets):

```bash
# For Prefect Cloud deployment
PREFECT_API_KEY=your_prefect_api_key
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/[ACCOUNT_ID]/workspaces/[WORKSPACE_ID]

# For cloud storage (if using S3/Azure/GCS)
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AZURE_STORAGE_CONNECTION_STRING=your_azure_connection
GCS_CREDENTIALS_JSON=your_gcs_json

# For Codecov (optional)
CODECOV_TOKEN=your_codecov_token
```

### Production Deployment

#### 1. Update prefect.yaml
- Set correct repository URL
- Configure schedules
- Set work pool name

#### 2. Deploy to Prefect Cloud
```bash
prefect cloud login
prefect deploy --all
```

#### 3. Start Production Worker
```bash
# Using systemd (Linux)
sudo systemctl start prefect-worker

# Using Docker
docker run -d \
  -e PREFECT_API_KEY=$PREFECT_API_KEY \
  -e PREFECT_API_URL=$PREFECT_API_URL \
  prefecthq/prefect:3-python3.10 \
  prefect worker start --pool default-agent-pool
```

### Docker Deployment

Create a `Dockerfile` for containerized deployments:

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN poetry install --no-dev --no-root

# Copy application code
COPY . .

# Install the package
RUN poetry install --no-dev

CMD ["poetry", "run", "python", "src/flows/main_pipeline.py"]
```

Build and run:
```bash
docker build -t weather-pipeline .
docker run weather-pipeline
```

### Kubernetes Deployment

Use Prefect's Kubernetes work pool for scalable deployments:

```bash
# Create Kubernetes work pool
prefect work-pool create k8s-pool --type kubernetes

# Deploy flows
prefect deploy --all

# Prefect will automatically create Kubernetes jobs
```

### Monitoring

#### Prefect UI

Monitor flow runs at:
- **Local**: http://localhost:4200
- **Cloud**: https://app.prefect.cloud

Features:
- 📊 Flow run history
- 🔔 Alerts and notifications
- 📈 Performance metrics
- 🐛 Error tracking and logs

#### GitHub Actions

Monitor CI/CD at:
- Repository → Actions tab
- View workflow runs, logs, and artifacts

#### Notifications

Configure Prefect notifications:

```python
from prefect.blocks.notifications import SlackWebhook

# In your flow
slack_webhook = SlackWebhook.load("my-slack-webhook")

@flow(on_failure=[slack_webhook])
def my_flow():
    ...
```

#### Logging

Flows automatically log to Prefect Cloud. Access logs:
1. Go to flow run in Prefect UI
2. Click "Logs" tab
3. Filter by level, task, or keyword

### Troubleshooting Prefect

**Flow fails to deploy**:
```bash
# Check Prefect connection
prefect cloud workspace ls

# Re-authenticate
prefect cloud login
```

**Worker not picking up runs**:
```bash
# Check work pool
prefect work-pool ls

# Verify worker is running
prefect worker ls
```

**GitHub Actions failing**:
- Check secrets are configured
- Verify Poetry version compatibility
- Review action logs for specific errors

### Best Practices

1. **Use separate environments** (dev, staging, prod)
2. **Tag deployments** with version numbers
3. **Monitor flow runs** regularly
4. **Set up alerts** for failures
5. **Use work queues** for prioritization
6. **Enable retries** for transient failures
7. **Cache expensive tasks** when appropriate

---

## Prefect Quick Start

### What Prefect Provides

✅ **Task Orchestration** - Automatic dependency management
✅ **Retry Logic** - Built-in error handling
✅ **Caching** - Avoid re-running expensive operations
✅ **Logging** - Complete execution history
✅ **State Management** - Track every task status

### Available Flows

#### 1. Demo Flow
```bash
poetry run python src/flows/demo_flow.py
```
Simple demonstration of Prefect features.

#### 2. Weather Ingestion Flow
```bash
poetry run python src/flows/weather_ingestion.py
```
Fetches weather data from NWS API and loads to Iceberg tables.

#### 3. dbt Transformations Flow
```bash
poetry run python src/flows/dbt_transformations.py
```
Runs dbt models, tests, and generates documentation.

#### 4. Complete Pipeline
```bash
poetry run python src/flows/main_pipeline.py
```
End-to-end: Ingestion → Transformation → Documentation

### Useful Prefect Commands

#### View Flow Runs
```bash
# List recent flow runs
poetry run prefect flow-run ls

# Get detailed info about a specific run
poetry run prefect flow-run inspect <flow-run-id>
```

#### View Logs
```bash
# View logs for a specific flow run
poetry run prefect flow-run logs <flow-run-id>
```

#### Start Prefect UI (Optional)
```bash
# Start a persistent Prefect server
poetry run prefect server start

# Then open http://localhost:4200 in your browser
```

The UI provides:
- 📊 Flow run visualization
- 📈 Performance metrics
- 🔍 Log exploration
- 📅 Schedule management

---

## Makefile Command Reference

### Most Common Commands

#### One-Command Setup
```bash
make start      # Start everything (Prefect + Catalog + Deploy flows)
make stop       # Stop all services
make status     # Check what's running
```

#### Run Flows
```bash
make demo              # Run demo flow
make run-weather       # Run weather ingestion
make run-dbt          # Run dbt transformations
make run-pipeline     # Run complete pipeline
```

### Complete Command List

#### Setup & Installation
```bash
make install    # Install dependencies
make setup      # Complete setup with verification
```

#### Server Management
```bash
make catalog-server    # Start catalog only
make prefect-server    # Start Prefect UI only
make deploy-flows      # Deploy flows only
make start            # Start all (recommended)
make stop             # Stop all
make status           # Check status
```

#### Data Operations
```bash
make fetch-stations      # Fetch station metadata
make fetch-observations  # Fetch weather data
make load-data          # Load to Iceberg
make fetch-all          # Do all of the above
```

#### dbt Commands
```bash
make dbt-run     # Run dbt models
make dbt-test    # Run dbt tests
make dbt-docs    # Generate documentation
make dbt-all     # Run all dbt tasks
```

#### Development
```bash
make lint      # Run linter
make format    # Format code
make test      # Run tests
```

#### Cleanup
```bash
make clean        # Clean generated files + stop servers
make clean-data   # Delete warehouse/data (WARNING!)
```

### Common Workflows

#### First Time Setup
```bash
make setup              # Install and verify
make start              # Start all servers
# Open http://localhost:4200
make demo               # Test with demo flow
```

#### Daily Development
```bash
make start              # Start services
make run-weather        # Fetch data
make dbt-run           # Transform data
make stop              # Clean shutdown
```

#### Full Pipeline Execution
```bash
make start              # Start services
make fetch-all         # Get all weather data
make run-pipeline      # Run complete pipeline
# Check http://localhost:4200 for results
make stop              # Stop when done
```

#### UI-Based Execution
```bash
make start              # Start + deploy flows
# Open http://localhost:4200/deployments
# Click "Run" on any deployment
# Watch execution in real-time
make stop              # Stop when done
```

### Tips

1. **Always use `make start`** to ensure everything runs properly
2. **Check logs** in `logs/` directory if something fails
3. **Use `make status`** to verify services are running
4. **Run `make stop`** before shutting down your machine
5. **PID files** (.prefect.pid, .catalog.pid) track running processes

### Troubleshooting Make Commands

**Services won't start?**
```bash
make stop      # Force stop everything
make clean     # Clean up
make start     # Try again
```

**Lost track of what's running?**
```bash
make status    # Check status
make stop      # Stop everything
make start     # Fresh start
```

**Need to reset everything?**
```bash
make clean          # Stop + cleanup
make clean-data     # Delete data (optional)
make setup          # Reinstall
make start          # Fresh start
```

### What Happens When...

#### `make start`
1. Starts Prefect server on port 4200
2. Starts Iceberg catalog on port 8181
3. Deploys flows for on-demand execution
4. Saves PIDs for tracking
5. Logs to `logs/` directory

#### `make stop`
1. Reads PID files
2. Kills processes gracefully
3. Cleans up PID files
4. Stops any lingering processes

#### `make demo`
1. Sets Prefect API URL
2. Runs demo flow
3. Sends to Prefect server
4. You see it in UI at http://localhost:4200

### Service Ports

- **Prefect UI**: http://localhost:4200
- **Catalog API**: http://localhost:8181
- **Deployments**: http://localhost:4200/deployments
- **API Docs**: http://localhost:4200/docs

### Shortcuts

```bash
make all     # Alias for 'start'
make run     # Alias for 'demo'
make deploy  # Alias for 'deploy-flows'
```

### Environment Variables

The Makefile automatically sets:
- `PREFECT_API_URL=http://0.0.0.0:4200/api`

You can override by exporting before running make:
```bash
export PREFECT_API_URL=https://api.prefect.cloud
make demo
```

---

Built with ❤️ for learning Apache Iceberg internals
- Add to memory