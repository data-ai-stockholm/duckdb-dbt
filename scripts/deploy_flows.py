"""Deploy Prefect flows for on-demand execution from UI."""

from prefect import serve
from src.flows.demo_flow import demo_pipeline
from src.flows.weather_ingestion import weather_ingestion_flow
from src.flows.dbt_transformations import dbt_transformations_flow
from src.flows.main_pipeline import main_pipeline_flow


if __name__ == "__main__":
    # Create deployments for all flows
    demo_deployment = demo_pipeline.to_deployment(
        name="demo-on-demand",
        tags=["demo", "on-demand"],
        description="Demo pipeline - click 'Run' to execute on-demand"
    )

    weather_deployment = weather_ingestion_flow.to_deployment(
        name="weather-ingestion-on-demand",
        tags=["weather", "ingestion", "on-demand"],
        description="Fetch and load weather data - click 'Run' to execute"
    )

    dbt_deployment = dbt_transformations_flow.to_deployment(
        name="dbt-transformations-on-demand",
        tags=["dbt", "transformations", "on-demand"],
        description="Run dbt models and tests - click 'Run' to execute",
        parameters={"profiles_dir": "dbt"}
    )

    pipeline_deployment = main_pipeline_flow.to_deployment(
        name="complete-pipeline-on-demand",
        tags=["pipeline", "full", "on-demand"],
        description="Complete end-to-end pipeline - click 'Run' to execute"
    )

    # Serve all deployments
    print("\n" + "=" * 70)
    print("🚀 STARTING PREFECT DEPLOYMENTS")
    print("=" * 70)
    print("\nDeployments created:")
    print("  1. demo-on-demand")
    print("  2. weather-ingestion-on-demand")
    print("  3. dbt-transformations-on-demand")
    print("  4. complete-pipeline-on-demand")
    print("\n" + "=" * 70)
    print("✨ Prefect UI: http://localhost:4200")
    print("   Go to 'Deployments' tab to run flows on-demand")
    print("=" * 70 + "\n")

    serve(
        demo_deployment,
        weather_deployment,
        dbt_deployment,
        pipeline_deployment
    )