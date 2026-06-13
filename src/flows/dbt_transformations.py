"""Prefect flow for dbt transformations."""

import subprocess

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


def _run_dbt(command: list[str], profiles_dir: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a dbt command and return the result."""
    return subprocess.run(
        ["dbt", *command, "--project-dir", "dbt", "--profiles-dir", profiles_dir],
        capture_output=True, text=True, check=check, cwd=".",
    )


@task(name="dbt-run", description="Run dbt models", retries=2, retry_delay_seconds=30,
      task_run_name="dbt-run-{profiles_dir}")
def dbt_run(profiles_dir: str = "dbt"):
    """Run dbt models."""
    print("Running dbt models...")
    try:
        result = _run_dbt(["run"], profiles_dir)
        print("✓ dbt run completed")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt run failed: {e.stderr}")
        raise


@task(name="dbt-test", description="Run dbt tests", retries=1, retry_delay_seconds=30)
def dbt_test(profiles_dir: str = "dbt"):
    """Run dbt tests."""
    print("Running dbt tests...")
    result = _run_dbt(["test"], profiles_dir, check=False)
    print("✓ All dbt tests passed" if result.returncode == 0 else "⚠️ Some dbt tests failed")
    return {"stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode}


@task(name="dbt-docs-generate", description="Generate dbt documentation")
def dbt_docs_generate(profiles_dir: str = "dbt"):
    """Generate dbt documentation."""
    print("Generating dbt documentation...")
    try:
        result = _run_dbt(["docs", "generate"], profiles_dir)
        print("✓ Documentation generated")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ docs generate failed: {e.stderr}")
        raise


@flow(name="dbt-transformations", description="Run dbt transformations and tests", log_prints=True)
def dbt_transformations_flow(profiles_dir: str = "dbt"):
    """Run dbt models, tests, and generate docs."""
    print("Starting dbt transformations...")

    run_result = dbt_run(profiles_dir)
    print("dbt models completed:")
    print(run_result)

    test_result = dbt_test(profiles_dir)
    print(f"dbt tests completed with return code: {test_result['returncode']}")
    print(test_result["stdout"])
    if test_result["returncode"] != 0:
        print("⚠️  Some tests failed:")
        print(test_result["stderr"])

    docs_result = dbt_docs_generate(profiles_dir)
    print("dbt documentation generated")

    test_passed = test_result["returncode"] == 0
    create_markdown_artifact(
        key="dbt-run-results",
        markdown=f"""# dbt Transformation Results

## Models Run
```
{run_result}
```

## Tests
**Status**: {"✅ Passed" if test_passed else "❌ Failed"}

```
{test_result["stdout"]}
```

## Documentation
Documentation generated successfully.
""",
        description="dbt transformation results",
    )

    return {
        "status": "success" if test_passed else "completed_with_warnings",
        "run_output": run_result,
        "test_result": test_result,
        "docs_output": docs_result,
    }


if __name__ == "__main__":
    dbt_transformations_flow()