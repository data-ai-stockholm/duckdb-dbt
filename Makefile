.PHONY: help install setup clean test lint format
.PHONY: catalog-server prefect-server prefect-stop deploy-flows
.PHONY: demo run-weather run-dbt run-pipeline
.PHONY: run-now run-now-weather run-now-dbt run-now-pipeline
.PHONY: fetch-data load-data dbt-run dbt-test
.PHONY: all start stop status

.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Configuration
PREFECT_API_URL := http://0.0.0.0:4200/api
CATALOG_PORT := 8181
PREFECT_PORT := 4200

##@ Setup & Installation

install: ## Install dependencies with Poetry
	@echo "$(BLUE)Installing dependencies...$(NC)"
	poetry install
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

setup: install ## Complete setup (install + verify)
	@echo "$(BLUE)Verifying setup...$(NC)"
	poetry run python -c "import prefect; print('Prefect version:', prefect.__version__)"
	poetry run dbt --version
	@echo "$(GREEN)✓ Setup complete!$(NC)"

##@ Servers

catalog-server: ## Start Iceberg REST catalog server (background)
	@echo "$(BLUE)Starting Iceberg REST catalog server...$(NC)"
	@poetry run catalog-server > logs/catalog.log 2>&1 & echo $$! > .catalog.pid
	@sleep 2
	@echo "$(GREEN)✓ Catalog server running on http://localhost:$(CATALOG_PORT)$(NC)"
	@echo "  PID: $$(cat .catalog.pid)"
	@echo "  Logs: logs/catalog.log"

prefect-server: ## Start Prefect UI server (background)
	@echo "$(BLUE)Starting Prefect server...$(NC)"
	@mkdir -p logs
	@PREFECT_API_URL=$(PREFECT_API_URL) poetry run prefect server start --host 0.0.0.0 > logs/prefect.log 2>&1 & echo $$! > .prefect.pid
	@sleep 3
	@echo "$(GREEN)✓ Prefect server running on http://localhost:$(PREFECT_PORT)$(NC)"
	@echo "  PID: $$(cat .prefect.pid)"
	@echo "  Logs: logs/prefect.log"

deploy-flows: ## Deploy Prefect flows for on-demand execution
	@echo "$(BLUE)Deploying Prefect flows...$(NC)"
	@mkdir -p logs
	@export PREFECT_API_URL=$(PREFECT_API_URL) && \
		poetry run python scripts/deploy_flows.py > logs/deploy.log 2>&1 & echo $$! > .deploy.pid
	@sleep 3
	@echo "$(GREEN)✓ Flows deployed and serving$(NC)"
	@echo "  View at: http://localhost:$(PREFECT_PORT)/deployments"
	@echo "  PID: $$(cat .deploy.pid)"

start: prefect-server catalog-server deploy-flows ## Start all servers (Prefect + Catalog + Deploy flows)
	@echo ""
	@echo "$(GREEN)======================================"
	@echo "✓ ALL SERVICES STARTED"
	@echo "======================================$(NC)"
	@echo "📊 Prefect UI:  http://localhost:$(PREFECT_PORT)"
	@echo "🗄️  Catalog:     http://localhost:$(CATALOG_PORT)"
	@echo "📦 Deployments: http://localhost:$(PREFECT_PORT)/deployments"
	@echo ""
	@echo "Run 'make stop' to stop all services"

stop: ## Stop all running servers
	@echo "$(YELLOW)Stopping all services...$(NC)"
	@if [ -f .prefect.pid ]; then kill $$(cat .prefect.pid) 2>/dev/null || true; rm .prefect.pid; echo "  ✓ Prefect stopped"; fi
	@if [ -f .catalog.pid ]; then kill $$(cat .catalog.pid) 2>/dev/null || true; rm .catalog.pid; echo "  ✓ Catalog stopped"; fi
	@if [ -f .deploy.pid ]; then kill $$(cat .deploy.pid) 2>/dev/null || true; rm .deploy.pid; echo "  ✓ Deploy stopped"; fi
	@pkill -f "prefect server start" 2>/dev/null || true
	@pkill -f "catalog-server" 2>/dev/null || true
	@pkill -f "deploy_flows.py" 2>/dev/null || true
	@echo "$(GREEN)✓ All services stopped$(NC)"

status: ## Check status of all services
	@echo "$(BLUE)Service Status:$(NC)"
	@if [ -f .prefect.pid ] && ps -p $$(cat .prefect.pid) > /dev/null 2>&1; then \
		echo "  $(GREEN)✓$(NC) Prefect server (PID: $$(cat .prefect.pid))"; \
	else \
		echo "  $(RED)✗$(NC) Prefect server (not running)"; \
	fi
	@if [ -f .catalog.pid ] && ps -p $$(cat .catalog.pid) > /dev/null 2>&1; then \
		echo "  $(GREEN)✓$(NC) Catalog server (PID: $$(cat .catalog.pid))"; \
	else \
		echo "  $(RED)✗$(NC) Catalog server (not running)"; \
	fi
	@if [ -f .deploy.pid ] && ps -p $$(cat .deploy.pid) > /dev/null 2>&1; then \
		echo "  $(GREEN)✓$(NC) Flow deployment (PID: $$(cat .deploy.pid))"; \
	else \
		echo "  $(RED)✗$(NC) Flow deployment (not running)"; \
	fi

##@ Flows & Demos

demo: ## Run demo Prefect flow
	@echo "$(BLUE)Running demo flow...$(NC)"
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python src/flows/demo_flow.py

run-weather: ## Run weather data ingestion flow
	@echo "$(BLUE)Running weather ingestion flow...$(NC)"
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python src/flows/weather_ingestion.py

run-dbt: ## Run dbt transformations flow
	@echo "$(BLUE)Running dbt transformations flow...$(NC)"
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python src/flows/dbt_transformations.py

run-pipeline: ## Run complete end-to-end pipeline
	@echo "$(BLUE)Running complete pipeline...$(NC)"
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python src/flows/main_pipeline.py

##@ "Run Now" - Trigger & Watch Dashboard

run-now: ## Run and watch demo flow with live dashboard (auto-starts server)
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python scripts/run_and_watch.py demo

run-now-weather: ## Run and watch weather flow with live dashboard
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python scripts/run_and_watch.py weather

run-now-dbt: ## Run and watch dbt flow with live dashboard
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python scripts/run_and_watch.py dbt

run-now-pipeline: ## Run and watch complete pipeline with live dashboard
	@export PREFECT_API_URL=$(PREFECT_API_URL) && poetry run python scripts/run_and_watch.py pipeline

##@ Data Operations

fetch-stations: ## Fetch weather station metadata
	@echo "$(BLUE)Fetching weather stations...$(NC)"
	@poetry run fetch-stations

fetch-observations: ## Fetch weather observations
	@echo "$(BLUE)Fetching weather observations...$(NC)"
	@poetry run fetch-observations

load-data: ## Load observations to Iceberg
	@echo "$(BLUE)Loading data to Iceberg...$(NC)"
	@poetry run load-observations

fetch-all: fetch-stations fetch-observations load-data ## Fetch and load all weather data
	@echo "$(GREEN)✓ All data fetched and loaded$(NC)"

##@ dbt Commands

dbt-run: ## Run dbt models
	@echo "$(BLUE)Running dbt models...$(NC)"
	@poetry run dbt run --project-dir dbt --profiles-dir dbt

dbt-test: ## Run dbt tests
	@echo "$(BLUE)Running dbt tests...$(NC)"
	@poetry run dbt test --project-dir dbt --profiles-dir dbt

dbt-docs: ## Generate dbt documentation
	@echo "$(BLUE)Generating dbt documentation...$(NC)"
	@poetry run dbt docs generate --project-dir dbt --profiles-dir dbt

dbt-all: dbt-run dbt-test dbt-docs ## Run all dbt commands
	@echo "$(GREEN)✓ All dbt tasks completed$(NC)"

##@ Development

lint: ## Run code linting
	@echo "$(BLUE)Running linter...$(NC)"
	@poetry run ruff check src/

format: ## Format code
	@echo "$(BLUE)Formatting code...$(NC)"
	@poetry run ruff format src/

test: ## Run tests
	@echo "$(BLUE)Running tests...$(NC)"
	@poetry run pytest tests/ -v

##@ Cleanup

clean: stop ## Clean up generated files and stop servers
	@echo "$(YELLOW)Cleaning up...$(NC)"
	@rm -rf target/ dbt_packages/ logs/
	@rm -rf .pytest_cache/ .ruff_cache/ .mypy_cache/
	@rm -rf **/__pycache__/
	@rm -f .catalog.pid .prefect.pid .deploy.pid
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

clean-data: ## Clean data files (WARNING: deletes warehouse and ingestion data)
	@echo "$(RED)WARNING: This will delete all data files!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf warehouse/ ingestion_data/; \
		echo "$(GREEN)✓ Data cleaned$(NC)"; \
	else \
		echo "$(YELLOW)Cancelled$(NC)"; \
	fi

##@ Shortcuts

all: start ## Start everything (alias for 'start')

run: demo ## Quick run demo (alias for 'demo')

deploy: deploy-flows ## Deploy flows (alias for 'deploy-flows')

##@ Help

help: ## Display this help message
	@echo "$(BLUE)"
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║          Weather Data Pipeline - Makefile Commands        ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo "$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf ""} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(YELLOW)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(BLUE)Quick Start:$(NC)"
	@echo "  1. make setup              # Install dependencies"
	@echo "  2. make start              # Start all servers"
	@echo "  3. Open http://localhost:4200 in browser"
	@echo "  4. make demo               # Run demo flow"
	@echo "  5. make stop               # Stop all servers"
	@echo ""