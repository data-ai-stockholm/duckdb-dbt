"""Prefect flow for dbt transformations."""

import subprocess

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


@task(
    name="dbt-run",
    description="Run dbt models",
    retries=2,
    retry_delay_seconds=30,
    task_run_name="dbt-run-{profiles_dir}",
)
def dbt_run(profiles_dir: str = "dbt"):
    """Run dbt models."""
    print("Running dbt models...")
    try:
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "dbt", "--profiles-dir", profiles_dir],
            capture_output=True,
            text=True,
            check=True,
            cwd=".",
        )
        print("✓ dbt run completed")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt run failed: {e.stderr}")
        raise


@task(name="dbt-test", description="Run dbt tests", retries=1, retry_delay_seconds=30)
def dbt_test(profiles_dir: str = "dbt"):
    """Run dbt tests."""
    print("Running dbt tests...")
    result = subprocess.run(
        ["dbt", "test", "--project-dir", "dbt", "--profiles-dir", profiles_dir],
        capture_output=True,
        text=True,
        check=False,  # Don't fail on test failures
        cwd=".",
    )

    if result.returncode == 0:
        print("✓ All dbt tests passed")
    else:
        print("⚠️ Some dbt tests failed")

    return {"stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode}


@task(
    name="dbt-docs-generate",
    description="Generate dbt documentation",
)
def dbt_docs_generate(profiles_dir: str = "dbt"):
    """Generate dbt documentation."""
    print("Generating dbt documentation...")
    try:
        result = subprocess.run(
            ["dbt", "docs", "generate", "--project-dir", "dbt", "--profiles-dir", profiles_dir],
            capture_output=True,
            text=True,
            check=True,
            cwd=".",
        )
        print("✓ Documentation generated")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ docs generate failed: {e.stderr}")
        raise


@flow(name="dbt-transformations", description="Run dbt transformations and tests", log_prints=True)
def dbt_transformations_flow(profiles_dir: str = "dbt"):
    """
    Main flow for dbt transformations.

    Steps:
    1. Run dbt models
    2. Run dbt tests
    3. Generate documentation
    """
    print("Starting dbt transformations...")

    # Run dbt models
    run_result = dbt_run(profiles_dir)
    print("dbt models completed:")
    print(run_result)

    # Run tests
    test_result = dbt_test(profiles_dir)
    print(f"dbt tests completed with return code: {test_result['returncode']}")
    print(test_result["stdout"])

    if test_result["returncode"] != 0:
        print("⚠️  Some tests failed:")
        print(test_result["stderr"])

    # Generate docs
    docs_result = dbt_docs_generate(profiles_dir)
    print("dbt documentation generated")

    # Create markdown artifact with results
    markdown_report = f"""# dbt Transformation Results

## Models Run
```
{run_result}
```

## Tests
**Status**: {"✅ Passed" if test_result["returncode"] == 0 else "❌ Failed"}

```
{test_result["stdout"]}
```

## Documentation
Documentation generated successfully.
"""

    create_markdown_artifact(
        key="dbt-run-results", markdown=markdown_report, description="dbt transformation results"
    )

    return {
        "status": "success" if test_result["returncode"] == 0 else "completed_with_warnings",
        "run_output": run_result,
        "test_result": test_result,
        "docs_output": docs_result,
    }


if __name__ == "__main__":
    dbt_transformations_flow()
