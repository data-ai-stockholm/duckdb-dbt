"""Simple demo Prefect flow to showcase orchestration capabilities."""

from datetime import timedelta
from time import sleep

from prefect import flow, task
from prefect.tasks import task_input_hash


@task(
    name="greet-user",
    description="Greet the user with a friendly message",
    retries=2,
    retry_delay_seconds=5,
)
def greet_user(name: str = "Data Engineer"):
    """Greet the user."""
    print(f"👋 Hello, {name}!")
    return f"Greeted {name}"


@task(
    name="fetch-data",
    description="Simulate fetching data from an API",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=5),
)
def fetch_data(source: str):
    """Simulate data fetching."""
    print(f"📥 Fetching data from {source}...")
    sleep(1)  # Simulate network delay
    data = {"source": source, "records": 100, "status": "success"}
    print(f"✅ Fetched {data['records']} records from {source}")
    return data


@task(name="process-data", description="Transform and process the data", retries=1)
def process_data(data: dict):
    """Process the fetched data."""
    print(f"🔄 Processing {data['records']} records...")
    sleep(0.5)  # Simulate processing
    processed = {**data, "processed": True, "timestamp": "2025-12-06T10:00:00Z"}
    print("✅ Processing complete!")
    return processed


@task(name="save-data", description="Save processed data to storage")
def save_data(data: dict):
    """Save the processed data."""
    print("💾 Saving data to warehouse...")
    sleep(0.3)  # Simulate write operation
    print("✅ Data saved successfully!")
    return {"saved": True, "location": "warehouse/demo_data"}


@flow(
    name="demo-pipeline",
    description="Demo flow showcasing Prefect orchestration features",
    log_prints=True,
)
def demo_pipeline(user_name: str = "Weather Data Engineer"):
    """
    Demonstrate Prefect flow orchestration with tasks.

    This flow shows:
    - Task dependencies and sequential execution
    - Retry logic
    - Caching
    - Logging
    - Task state management
    """
    print("\n" + "=" * 70)
    print("🚀 PREFECT DEMO PIPELINE - STARTING")
    print("=" * 70 + "\n")

    # Task 1: Greet user
    greeting_result = greet_user(user_name)

    # Task 2: Fetch data (this will be cached on subsequent runs)
    data_result = fetch_data("National Weather Service API")

    # Task 3: Process data (depends on fetch_data)
    processed_data = process_data(data_result)

    # Task 4: Save data (depends on process_data)
    save_result = save_data(processed_data)

    # Summary
    print("\n" + "=" * 70)
    print("✨ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print("\nSummary:")
    print(f"  • Greeting: {greeting_result}")
    print(f"  • Records fetched: {data_result['records']}")
    print(f"  • Processing status: {processed_data['processed']}")
    print(f"  • Save location: {save_result['location']}")
    print("\n" + "=" * 70 + "\n")

    return {
        "status": "success",
        "greeting": greeting_result,
        "data": data_result,
        "processed": processed_data,
        "saved": save_result,
    }


if __name__ == "__main__":
    # Run the demo flow
    result = demo_pipeline()

    print("\n🎉 Flow execution complete!")
    print(f"Result: {result['status']}")
