#!/bin/bash

# Script to run flows with Prefect UI

# Set Prefect API URL to use the local server
export PREFECT_API_URL=http://0.0.0.0:4200/api

echo "======================================"
echo "Running flows with Prefect UI"
echo "======================================"
echo ""
echo "Prefect UI available at: http://localhost:4200"
echo ""

# Run the flow passed as argument, or demo flow by default
FLOW=${1:-"src/flows/demo_flow.py"}

echo "Running flow: $FLOW"
poetry run python "$FLOW"

echo ""
echo "View results at: http://localhost:4200"