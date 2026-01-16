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
import time
import os
import webbrowser
import subprocess
from typing import Optional
from urllib.parse import urljoin

import httpx
from prefect.client.orchestration import get_client
from prefect.client.schemas.responses import FlowRun


# Configuration
PREFECT_API_URL = "http://0.0.0.0:4200/api"
PREFECT_UI_URL = "http://localhost:4200"
DEPLOYMENT_MAPPING = {
    "demo": "demo-flow/demo-deployment",
    "weather": "weather-ingestion/weather-ingestion-deployment",
    "dbt": "dbt-transformations/dbt-transformations-deployment",
    "pipeline": "weather-pipeline/weather-pipeline-deployment",
}


async def ensure_server_running() -> bool:
    """Check if Prefect server is running, try to start if not."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{PREFECT_API_URL}/health", timeout=2.0)
            return response.status_code == 200
    except Exception:
        print("⚠️  Prefect server not running. Starting...")
        try:
            subprocess.Popen(
                ["make", "prefect-server"],
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            await asyncio.sleep(4)
            return True
        except Exception as e:
            print(f"❌ Failed to start Prefect server: {e}")
            return False


async def trigger_flow(deployment_name: str) -> Optional[str]:
    """Trigger a flow deployment and return the flow run ID."""
    try:
        async with get_client(api_url=PREFECT_API_URL) as client:
            # Get deployment
            deployments = await client.read_deployments(
                deployment_filter={
                    "name": {"like_": f"%{deployment_name.split('/')[-1]}%"}
                }
            )

            if not deployments:
                print(f"❌ Deployment '{deployment_name}' not found")
                return None

            deployment_id = deployments[0].id

            # Create flow run
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id,
                tags=["cli-triggered"],
            )

            print(f"✓ Flow run created: {flow_run.id}")
            return str(flow_run.id)
    except Exception as e:
        print(f"❌ Error triggering flow: {e}")
        return None


async def watch_flow_run(flow_run_id: str) -> None:
    """Watch flow run execution and stream logs."""
    print(f"\n📊 Watching flow execution...")
    print(f"🔗 Live Dashboard: {PREFECT_UI_URL}/flow-runs/{flow_run_id}\n")

    # Open dashboard in browser
    webbrowser.open(f"{PREFECT_UI_URL}/flow-runs/{flow_run_id}")

    try:
        async with get_client(api_url=PREFECT_API_URL) as client:
            previous_state_id = None
            last_log_time = 0

            while True:
                # Get flow run details
                flow_run = await client.read_flow_run(flow_run_id)

                # Display state changes
                if flow_run.state_id != previous_state_id:
                    previous_state_id = flow_run.state_id
                    state_icon = {
                        "PENDING": "⏳",
                        "RUNNING": "▶️ ",
                        "COMPLETED": "✅",
                        "FAILED": "❌",
                        "CANCELLED": "⚠️ ",
                    }.get(flow_run.state.type, "•")
                    print(f"{state_icon} State: {flow_run.state.type}")

                # Fetch and display logs
                logs_response = await client.read_logs(
                    flow_run_id=flow_run_id,
                    log_filter={
                        "level": {"value_": {"ANY": [20, 30, 40, 50]}}  # INFO and above
                    },
                )

                if logs_response:
                    for log in logs_response:
                        log_time = log.timestamp.timestamp()
                        if log_time > last_log_time:
                            level_icon = {
                                20: "ℹ️ ",  # INFO
                                30: "⚠️ ",  # WARNING
                                40: "❌",  # ERROR
                                50: "💥",  # CRITICAL
                            }.get(log.level, "•")
                            print(f"  {level_icon} {log.message}")
                            last_log_time = log_time

                # Check if completed
                if flow_run.state.type in ["COMPLETED", "FAILED", "CANCELLED"]:
                    print(f"\n{'='*60}")
                    print(f"Flow run finished: {flow_run.state.type}")
                    print(f"{'='*60}")
                    break

                await asyncio.sleep(1)

    except Exception as e:
        print(f"⚠️  Error watching flow: {e}")


def select_flow() -> str:
    """Prompt user to select a flow."""
    print("\n📦 Available Flows:")
    for i, flow_name in enumerate(DEPLOYMENT_MAPPING.keys(), 1):
        print(f"  {i}. {flow_name}")

    while True:
        try:
            choice = input("\nSelect flow (1-4): ").strip()
            flow_names = list(DEPLOYMENT_MAPPING.keys())
            selected = flow_names[int(choice) - 1]
            return selected
        except (ValueError, IndexError):
            print("Invalid choice. Please try again.")


async def main():
    """Main entry point."""
    import asyncio

    # Get flow name from CLI or prompt
    if len(sys.argv) > 1:
        flow_name = sys.argv[1].lower()
        if flow_name not in DEPLOYMENT_MAPPING:
            print(f"❌ Unknown flow: {flow_name}")
            print(f"Available: {', '.join(DEPLOYMENT_MAPPING.keys())}")
            sys.exit(1)
    else:
        flow_name = select_flow()

    deployment_name = DEPLOYMENT_MAPPING[flow_name]

    print(f"\n🚀 Running {flow_name} flow...")
    print(f"📍 Deployment: {deployment_name}")

    # Ensure server is running
    if not await ensure_server_running():
        print("❌ Could not start Prefect server")
        sys.exit(1)

    # Trigger flow
    flow_run_id = await trigger_flow(deployment_name)
    if not flow_run_id:
        sys.exit(1)

    # Watch execution
    await watch_flow_run(flow_run_id)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
