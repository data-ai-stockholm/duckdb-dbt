# Weather Data Platform

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/dependency-poetry-purple)](https://python-poetry.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.4.2+-yellow)](https://duckdb.org/)
[![dbt](https://img.shields.io/badge/dbt-1.10+-orange)](https://www.getdbt.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.53+-red)](https://streamlit.io/)
[![Apache Iceberg](https://img.shields.io/badge/Iceberg-REST-green)](https://iceberg.apache.org/)

Production-ready data engineering project demonstrating **DuckDB**, **dbt**, **Apache Iceberg**, **Prefect orchestration**, and **Streamlit visualization** with a unified web UI.

> 🎯 **End-to-end weather data pipeline** from API ingestion through transformation to interactive visualization

## Quick Start

### One Command to Launch

```bash
poetry run streamlit run app.py
```

Opens: **http://localhost:8501** 🌤️

That's it! Everything you need is in one unified web UI.

## Features

✅ **Real-time Analytics Dashboard** - Interactive visualizations with Plotly
✅ **Pipeline Monitoring** - Architecture, record flow, status tracking
✅ **Data Quality Metrics** - NULL checks, statistics, uniqueness validation
✅ **Complete Lineage** - dbt model dependencies and documentation
✅ **Apache Iceberg** - Production-grade table format with REST catalog
✅ **DuckDB** - Native Iceberg support with v1.4.2+
✅ **dbt** - 5 production models (staging + 4 marts)
✅ **Prefect** - Workflow orchestration at http://localhost:4200
✅ **Multi-Cloud** - S3, Azure Blob Storage, GCS support

## What You Get

### 📊 Unified Streamlit UI (Single Port: 8501)

Navigate via sidebar with 5 pages:

| Page | What It Shows |
|------|---------------|
| 📊 **Analytics Overview** | Summary metrics, station data, real-time stats |
| 📈 **Trends & Analysis** | Temperature/humidity trends, weather conditions, anomalies |
| 🔗 **Pipeline Monitoring** | Architecture diagram, record flow (140→40), pipeline status |
| 🧪 **Data Quality** | NULL checks (100% passing), statistics, uniqueness |
| 📋 **Lineage Details** | dbt models, sources, complete dependency graph |

### 📈 Data Metrics

- **140** weather observations across **5 US stations** (KJFK, KLAX, KORD, KDFW, KATL)
- **Temperature range**: 25.8°C → 74.8°C (avg 50.8°C)
- **Humidity**: 30-90%
- **56 anomalies** detected via Z-score analysis
- **40 daily aggregates** from 8 unique dates
- **100%** non-NULL critical fields

### 🔄 Data Flow

```
Raw Observations (140)
    ↓ [dbt staging]
Cleaned Data (140)
    ↓ [dbt marts]
Analytics Tables:
  • fact_observations (140)
  • fact_daily_weather (40)
  • dim_stations (5)
  • extreme_weather_events (56)
    ↓ [Streamlit visualization]
Interactive Web UI (http://localhost:8501)
```

## Architecture

```
Data Ingestion
    ↓ (NWS API)
DuckDB + Iceberg
    ↓ (weather.duckdb)
dbt Transformations
    ├─ stg_observations (cleaning)
    ├─ fact_observations (facts)
    ├─ fact_daily_weather (aggregates)
    ├─ dim_stations (dimension)
    └─ extreme_weather_events (anomalies)
    ↓
Streamlit Dashboard (app.py)
    ├─ Analytics
    ├─ Trends
    ├─ Pipeline Status
    ├─ Data Quality
    └─ Lineage

(Optional) Prefect Orchestration @ localhost:4200
(Optional) dbt Docs @ localhost:8000
```

## Project Structure

```
duckdb-dbt/
├── app.py                              ✨ MAIN: Unified Streamlit UI (779 lines)
├── dbt_lineage_flow.py                 Prefect lineage monitoring
├── weather.duckdb                      📊 Database with all data
├── dbt/
│   ├── models/
│   │   ├── staging/stg_observations.sql
│   │   └── marts/
│   │       ├── fact_observations.sql
│   │       ├── fact_daily_weather.sql
│   │       ├── dim_stations.sql
│   │       └── extreme_weather_events.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── src/
│   ├── catalog/rest_server.py          Iceberg REST API
│   ├── flows/                          Prefect workflows
│   ├── ingestion/                      Data loading
│   └── models/                         dbt integration
├── config/
│   └── storage.yaml                    Multi-cloud config
├── docs/
│   ├── README.md                       User guide (you are here)
│   └── CLAUDE.md                       Technical deep-dive
└── pyproject.toml                      Dependencies
```

## Installation

```bash
# Install dependencies
poetry install

# Run the app
poetry run streamlit run app.py
```

Then open: **http://localhost:8501**

## Optional Services

### View dbt Lineage Documentation

```bash
cd dbt
dbt docs serve
```

Opens: **http://localhost:8000**

### Access Prefect Orchestration

```bash
# Already running in background, or start it:
poetry run prefect server start
```

Opens: **http://localhost:4200**

### Query Data Directly

```bash
poetry run duckdb weather.duckdb

> SELECT * FROM main_marts.fact_observations LIMIT 5;
> SELECT COUNT(*) FROM main_marts.fact_daily_weather;
```

## Configuration

Edit `config/storage.yaml` for storage backends:

```yaml
storage:
  backend: local              # or: s3, azure, gcs

table_format: iceberg         # Apache Iceberg format

iceberg:
  catalog:
    type: rest
    uri: http://localhost:8181
```

## Transformations

### dbt Models

**Staging Layer:**
- `stg_observations` - Cleaned weather data with unit conversions (°F→°C, mph→m/s)

**Marts Layer:**
- `fact_observations` - 140 enriched observations with temporal attributes
- `fact_daily_weather` - 40 daily aggregates by station
- `dim_stations` - 5 station dimension table
- `extreme_weather_events` - 56 anomalies detected via Z-score

All models passing: **PASS=5** ✅

## Data Quality

✅ **NULL Checks**: 100% non-NULL for critical fields
✅ **Uniqueness**: 5 unique stations, 8 unique dates, 4 weather conditions
✅ **Range**: Temperature -5°C to 75°C (within expected weather range)
✅ **Aggregation**: Daily summaries correctly calculate avg/min/max
✅ **Anomaly Detection**: Z-score analysis identifies 56 extreme weather events

## Technology Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| **Data Storage** | DuckDB + Apache Iceberg | 1.4.2+ |
| **Transformation** | dbt | 1.10+ |
| **Visualization** | Streamlit + Plotly | 1.53+ / 6.5+ |
| **Orchestration** | Prefect | 3.6+ |
| **Language** | Python | 3.10+ |
| **Dependency Mgmt** | Poetry | Latest |

## Learn More

See **[CLAUDE.md](docs/CLAUDE.md)** for:
- Deep-dive into Apache Iceberg architecture
- REST catalog implementation details
- DuckDB + Iceberg integration
- Prefect orchestration setup
- GitHub Actions CI/CD
- Production deployment guide
- Advanced features & troubleshooting

## Next Steps

1. ✅ Explore the analytics dashboard
2. ✅ Check data quality metrics
3. ✅ View pipeline lineage
4. Optional: Deploy to production with Polaris
5. Optional: Set up scheduled Prefect flows

## Built With

- 🌤️ Real weather data from National Weather Service API
- 📊 DuckDB for OLAP analytics
- 🗂️ Apache Iceberg for reliable data lake
- 🔄 dbt for data transformations
- 🎨 Streamlit for interactive visualization
- 🚀 Prefect for workflow orchestration
- ⚙️ Poetry for dependency management

## License

MIT License - Open source and free to use

---

**Start exploring**: `poetry run streamlit run app.py` → http://localhost:8501 🚀
