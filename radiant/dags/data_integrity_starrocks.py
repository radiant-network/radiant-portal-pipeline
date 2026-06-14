"""Run the data_qa dbt test suite (StarRocks) from Airflow.

Run the dbt tests, then in parallel:
    - push the results to TestQuality as a JUnit report (optional, see below)
    - fail the run on test failures, ignoring known errors

The TestQuality push can be turned off per-run via the `skip_testquality_push` DAG param (default off, i.e. push
enabled), and is also skipped if the corresponding connection is missing.

The TestQuality connection (`testquality_conn`) should contain:
    - password: TestQuality access token (required)
    - host:     API base URL (optional, defaults to https://api.testquality.com)
    - extra:    fields for the body of the import_xml request, e.g.
                {"project_id": "1234", "run_name": "Clin QA - {date} - Data",
                 "plan_name": "my_plan", "milestone_name": "my_milestone"}

In the extra, project_id and run_name are required; other import_xml fields are optional.
In run_name, the {date} placeholder is replaced with the run date (DD/MM/YYYY).
See the upload_test_run command for the full list of fields:
https://github.com/BitModern/testQualityCli/blob/master/src/UploadTestRunCommand.ts

The task running dbt is based on the PythonVirtualenvOperator. It essentially mimic the local dbt setup,
i.e. we create a venv on the fly, inject expected environment variable, and then run the python commands.

The PythonVirtualenvOperator runs synchronously, so a long-running StarRocks query holds a worker until
it returns. We assume that this is acceptable for now. The dag can be paused to free workers.
"""

import logging
import os
import pathlib
import re

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, NAMESPACE

LOGGER = logging.getLogger(__name__)

DATA_QA_DIR = str(pathlib.Path(__file__).resolve().parent.parent / "data_qa")


TESTQUALITY_CONN_ID = "testquality_conn"
TESTQUALITY_DEFAULT_API_URL = "https://api.testquality.com"

# Jira project ids whose tagged failing tests are treated as known errors
KNOWN_ERROR_JIRA_PROJECTS = ("SJRA",)
# Matches a known error tag in a test name, e.g. SJRA-1234 (case-insensitive)
KNOWN_ERROR_TAG = re.compile(rf"(?:{'|'.join(KNOWN_ERROR_JIRA_PROJECTS)})-\d+", re.IGNORECASE)


# Create required environment variables for dbt
def export_starrocks_env(context) -> None:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("starrocks_conn")
    os.environ["SR_HOST"] = conn.host
    os.environ["SR_PORT"] = str(conn.port)
    os.environ["SR_USER"] = conn.login or "root"
    os.environ["SR_PASSWORD"] = conn.password or ""


# Clear environment variables once dbt has run
# Most executors run each task on it's own short-lived process (fork or pod), but we clean up for prevention.
def clear_starrocks_env(context, result=None) -> None:
    for var in ("SR_HOST", "SR_PORT", "SR_USER", "SR_PASSWORD"):
        os.environ.pop(var, None)


@dag(
    dag_id=f"{NAMESPACE}-data-integrity-starrocks",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["radiant", "dbt", "starrocks"],
    dag_display_name="Radiant - Data Integrity Checks (StarRocks)",
    params={
        "skip_testquality_push": Param(
            False,
            type="boolean",
            title="Skip TestQuality push",
            description="When enabled, the TestQuality upload task is skipped for this run.",
        ),
    },
)
def data_integrity_starrocks():
    @task.virtualenv(
        task_id="run_dbt",
        task_display_name="[VenvOp] Run dbt data tests",
        requirements=["dbt-core~=1.9.0", "dbt-starrocks~=1.9.0"],
        system_site_packages=False,  # avoid conflicts with airflow dependencies
        venv_cache_path=None,  # starts from a fresh venv; required to automatically cleanup files written by dbt
        pre_execute=export_starrocks_env,
        post_execute=clear_starrocks_env,
    )
    def run_dbt(data_qa_dir: str) -> str:
        """Run the data_qa dbt test suite against StarRocks.

        Returns dbt's run_results.json artifact (as dict) via XCom.
        """
        import json
        import os
        import shutil
        import sys
        from pathlib import Path

        from dbt.cli.main import dbtRunner

        os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"

        # When it is run, dbt writes files to disk.
        # To avoid polluting the worker filesystem, we work from a copy of the dbt project in the
        # virtual env. This assumes venv_cache_path=None (airflow's default).
        proj_dir = os.path.join(sys.prefix, "data_qa")
        shutil.copytree(
            data_qa_dir,
            proj_dir,
            ignore=shutil.ignore_patterns(".venv", "target", "dbt_packages", "logs", "__pycache__", ".env"),
        )

        runner = dbtRunner()

        global_flags = [
            "--use-colors",  # force ANSI colors as these won't be added by default as the worker stdout isn't a TTY
            "--printer-width",
            "120",
        ]
        command_flags = ["--project-dir", proj_dir, "--profiles-dir", proj_dir]

        deps = runner.invoke([*global_flags, "deps", *command_flags])
        if not deps.success:
            raise RuntimeError(f"`dbt deps` failed: {deps.exception}")

        res = runner.invoke([*global_flags, "test", *command_flags])
        if res.exception:
            raise RuntimeError(f"dbt itself failed to run (not a data-test failure): {res.exception}")

        run_results_path = Path(proj_dir) / "target" / "run_results.json"
        if not run_results_path.exists():
            raise RuntimeError("dbt produced no run_results.json — likely a StarRocks connection failure.")
        return json.loads(run_results_path.read_text(encoding="utf-8"))

    @task(
        task_id="upload_to_testquality",
        task_display_name="Upload JUnit report to TestQuality",
    )
    def upload_to_testquality(run_results_json: dict) -> None:
        """Convert dbt's run_results.json to JUnit XML and push it to TestQuality.

        Skips when the `skip_testquality_push` DAG param is enabled, or when the testquality_conn
        connection is absent.
        """
        import requests
        from airflow.exceptions import AirflowFailException, AirflowNotFoundException, AirflowSkipException
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context

        from radiant.data_qa.scripts.run_results_to_junit import to_junit_xml

        context = get_current_context()
        if context["params"]["skip_testquality_push"]:
            raise AirflowSkipException(
                "TestQuality upload disabled via the 'skip_testquality_push' DAG param — skipping."
            )

        try:
            conn = BaseHook.get_connection(TESTQUALITY_CONN_ID)
        except AirflowNotFoundException:
            raise AirflowSkipException(
                f"Connection '{TESTQUALITY_CONN_ID}' not configured — skipping TestQuality upload."
            ) from None

        junit_xml = to_junit_xml(run_results_json)

        extra = conn.extra_dejson
        missing = {"run_name", "project_id"} - extra.keys()
        if missing:
            raise AirflowFailException(
                f"testquality_conn extra is missing required keys: {', '.join(sorted(missing))}"
            )
        run_name = extra.pop("run_name").replace("{date}", context["logical_date"].strftime("%d/%m/%Y"))
        data = {"run_name": run_name, "filepath": "junit.xml", **extra}

        resp = requests.post(
            f"{conn.host or TESTQUALITY_DEFAULT_API_URL}/api/import_xml",  # CLI's default upload route
            headers={"Authorization": f"Bearer {conn.password}"},
            data=data,
            files={"file": ("junit.xml", junit_xml, "text/xml")},
            timeout=120,
        )
        resp.raise_for_status()
        LOGGER.info("TestQuality import_xml response: %s", resp.text)
        LOGGER.info("Uploaded JUnit report to TestQuality as run '%s'.", run_name)

    @task(
        task_id="check_qa_results",
        task_display_name="Assert no failing data tests",
    )
    def check_results(run_results_json: dict) -> None:
        """Fail the run if unexpected data tests failed

        Failing tests tagged with a Jira ticket (SJRA-XXXX) are known errors: they are reported
        but do not fail the run.
        """

        from collections import Counter

        from airflow.exceptions import AirflowFailException

        results = run_results_json.get("results", [])
        status_counts: Counter = Counter()
        known, unexpected = [], []
        for r in results:
            status = (r.get("status") or "").lower()
            status_counts[status] += 1
            if status in ("fail", "error"):
                uid = r["unique_id"]
                (known if KNOWN_ERROR_TAG.search(uid) else unexpected).append(uid)

        LOGGER.info(
            "%d passed · %d warned · %d failed — %d known, %d unexpected (of %d)",
            status_counts["pass"],
            status_counts["warn"],
            len(known) + len(unexpected),
            len(known),
            len(unexpected),
            len(results),
        )
        if known:
            LOGGER.warning("Known errors (tagged with a Jira ticket), not failing the run:\n%s", "\n".join(known))

        if unexpected:
            raise AirflowFailException(
                f"{len(unexpected)} DATA QA test(s) failed. "
                f"See the 'Run dbt data tests' task logs for the full dbt output.\n"
                f"Failed tests:\n" + "\n".join(unexpected)
            )

    run_results = run_dbt(data_qa_dir=DATA_QA_DIR)
    upload_to_testquality(run_results)
    check_results(run_results)


data_integrity_starrocks()
