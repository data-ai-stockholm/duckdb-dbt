"""Main Prefect flow orchestrating the complete weather data pipeline."""

from prefect import flow

from src.flows.dbt_transformations import dbt_transformations_flow
from src.flows.weather_ingestion import weather_ingestion_flow


@flow(
    name="weather-data-pipeline",
    description="Complete end-to-end weather data pipeline with ingestion and transformations",
    log_prints=True,
)
def main_pipeline_flow():
    """
    Main orchestration flow for the complete weather data pipeline.

    Steps:
    1. Ingest weather data (fetch + load to Iceberg)
    2. Run dbt transformations
    3. Generate analytics and documentation
    """
    print("=" * 70)
    print("WEATHER DATA PIPELINE - STARTING")
    print("=" * 70)

    # Step 1: Ingest weather data
    print("\n📥 Step 1: Data Ingestion")
    ingestion_result = weather_ingestion_flow()

    if ingestion_result["status"] != "success":
        raise Exception("Data ingestion failed")

    print("✅ Data ingestion completed successfully")

    # Step 2: Run dbt transformations
    print("\n🔄 Step 2: dbt Transformations")
    dbt_result = dbt_transformations_flow()

    print("✅ dbt transformations completed")

    # Summary
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)

    return {"status": "success", "ingestion": ingestion_result, "transformations": dbt_result}


if __name__ == "__main__":
    main_pipeline_flow()
