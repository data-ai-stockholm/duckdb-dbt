#!/usr/bin/env python
"""
Run Prefect flows and watch execution in real-time.

Usage:
    python scripts/run_and_watch.py [flow_name]

Flows:
    - demo (default)
    - weather
    - dbt
    - pipeline
"""

import sys
import os
import time
import webbrowser
import subprocess
from typing import Optional


# Configuration
PREFECT_API_URL = "http://0.0.0.0:4200/api"
PREFECT_UI_URL = "http://localhost:4200"
FLOWS = {
    "demo": "src.flows.demo_flow",
    "weather": "src.flows.weather_ingestion",
    "dbt": "src.flows.dbt_transformations",
    "pipeline": "src.flows.main_pipeline",
}


def check_server_running() -> bool:
    """Check if Prefect server is running."""
    try:
        import httpx
        with httpx.Client(timeout=2.0) as client:
            response = client.get(f"{PREFECT_API_URL}/health")
            return response.status_code == 200
    except Exception:
        return False


def start_prefect_server() -> None:
    """Start Prefect server."""
    print("⚠️  Prefect server not running. Starting...")
    subprocess.Popen(
        ["make", "prefect-server"],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print("⏳ Waiting for Prefect server to start...")
    time.sleep(5)


def select_flow() -> str:
    """Prompt user to select a flow."""
    print("\n📦 Available Flows:")
    for i, flow_name in enumerate(FLOWS.keys(), 1):
        print(f"  {i}. {flow_name}")

    while True:
        try:
            choice = input("\nSelect flow (1-4): ").strip()
            flow_names = list(FLOWS.keys())
            selected = flow_names[int(choice) - 1]
            return selected
        except (ValueError, IndexError):
            print("Invalid choice. Please try again.")


def run_flow(flow_name: str) -> None:
    """Run a flow and open the dashboard."""
    print(f"\n🚀 Running {flow_name} flow...")

    # Check if server is running
    if not check_server_running():
        start_prefect_server()

    # Get the flow module
    flow_module = FLOWS[flow_name]

    # Set API URL and run the flow
    env = os.environ.copy()
    env["PREFECT_API_URL"] = PREFECT_API_URL

    print(f"📍 Executing: {flow_module}")
    print(f"📊 Dashboard: {PREFECT_UI_URL}\n")

    # Open dashboard in browser
    webbrowser.open(PREFECT_UI_URL)

    # Run the flow
    try:
        result = subprocess.run(
            ["poetry", "run", "python", "-m", flow_module],
            env=env,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=False,
        )

        if result.returncode == 0:
            print(f"\n✅ {flow_name} flow completed successfully!")
            print(f"\n📊 View results at: {PREFECT_UI_URL}/flow-runs")
        else:
            print(f"\n❌ {flow_name} flow failed with exit code {result.returncode}")

    except KeyboardInterrupt:
        print("\n⚠️  Flow execution interrupted by user")
    except Exception as e:
        print(f"❌ Error running flow: {e}")


def main():
    """Main entry point."""
    # Get flow name from CLI or prompt
    if len(sys.argv) > 1:
        flow_name = sys.argv[1].lower()
        if flow_name not in FLOWS:
            print(f"❌ Unknown flow: {flow_name}")
            print(f"Available: {', '.join(FLOWS.keys())}")
            sys.exit(1)
    else:
        flow_name = select_flow()

    run_flow(flow_name)


if __name__ == "__main__":
    main()
