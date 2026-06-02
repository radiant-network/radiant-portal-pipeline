"""Run the data-qa dbt test suite (StarRocks) from Airflow.

dbt and its StarRocks adapter conflict with Airflow's own dependencies, so the
tests run in an isolated virtualenv (PythonVirtualenvOperator via @task.virtualenv)
that installs dbt at run time, independent of the Airflow runtime.

The StarRocks connection is exported as SR_* environment variables by a
pre_execute hook running in the worker process; the virtualenv subprocess
inherits them and profiles.yml reads them. Credentials never go through XCom
or rendered templates, so they never appear in the UI or the metadata database.

The bulk of the work runs in StarRocks, however, a long-running query can tie up a
worker while it waits for results. We assume this is fine for now — the dag can
be paused to free workers. If it becomes a problem, consider a dedicated pool or
a deferrable operator instead of the PythonVirtualenvOperator.
"""
import os
import pathlib

import pendulum
from airflow import DAG
from airflow.decorators import task

from radiant.dags import DEFAULT_ARGS, NAMESPACE

# The data-qa dbt project lives inside the radiant package (radiant/data-qa) so it
# travels with the dags in every environment (local volume, Docker image, MWAA S3
# sync). Resolved relative to this file: radiant/dags/ -> radiant/ -> data-qa.
DATA_QA_DIR = str(pathlib.Path(__file__).resolve().parent.parent / "data-qa")


def export_starrocks_env(context) -> None:
    """Export the starrocks_conn connection as the SR_* env vars dbt expects."""
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("starrocks_conn")
    os.environ["SR_HOST"] = conn.host
    os.environ["SR_PORT"] = str(conn.port or 9030)
    os.environ["SR_USER"] = conn.login or "root"
    os.environ["SR_PASSWORD"] = conn.password or ""


with DAG(
    dag_id=f"{NAMESPACE}-data-qa-starrocks",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["radiant", "dbt", "starrocks", "manual"],
    dag_display_name="Radiant - Data QA (StarRocks dbt tests)",
) as dag:

    @task.virtualenv(
        task_id="run_dbt_data_tests",
        task_display_name="[VenvOp] Run dbt data tests",
        requirements=["dbt-core~=1.9.0", "dbt-starrocks~=1.9.0"],
        system_site_packages=False,
        pre_execute=export_starrocks_env,
    )
    def run_dbt_data_tests(data_qa_dir: str) -> None:
        """Run the data-qa dbt test suite against StarRocks.

        SR_* env vars (host, port, user, password) are inherited from the worker
        process — see export_starrocks_env. profiles.yml reads them via env_var().
        """
        import os
        import shutil
        import sys

        os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"

        # We work on a copy placed at the root of the virtualenv (sys.prefix), which Airflow deletes
        # when the task ends. We do this because `dbt deps` writes files to disk and we want to avoid 
        # polluting the worker. Note: this relies on venv_cache_path=None (the default, a fresh venv per run).
        proj_dir = os.path.join(sys.prefix, "data-qa")
        shutil.copytree(
            data_qa_dir,
            proj_dir,
            ignore=shutil.ignore_patterns(".venv", "target", "dbt_packages", "logs", "__pycache__", ".env"),
        )

        from dbt.cli.main import dbtRunner

        runner = dbtRunner()
        base = ["--project-dir", proj_dir, "--profiles-dir", proj_dir]

        deps = runner.invoke(["deps", *base])
        if not deps.success:
            raise RuntimeError(f"`dbt deps` failed: {deps.exception}")

        res = runner.invoke(["test", *base])
        if not res.success:
            raise RuntimeError(f"dbt test failed: {res.exception or 'see log above for failing tests'}")

    run_dbt_data_tests(data_qa_dir=DATA_QA_DIR)
