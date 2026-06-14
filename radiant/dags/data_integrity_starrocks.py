"""Run the data_qa dbt test suite (StarRocks) from Airflow.

Run the dbt tests in a dedicated container (ECS or K8s) that uploads its results to S3,
then, reading those results back from S3:
- push the results to TestQuality as a JUnit report (optional, see below)
- inspect the results and fail the dag run on test failures, ignoring known errors


TestQuality push
----------------

Can be turned off per-run via the `skip_testquality_push` DAG param (default off, i.e. push
enabled), and is skipped if the `testquality_conn` connection is missing.

The connection should contain:
    - password: TestQuality access token (required)
    - host:     API base URL (optional, defaults to https://api.testquality.com)
    - extra:    fields for the import_xml request body, e.g.
        {"project_id": "1234", "run_name": "Clin QA - {date} - Data", ...}

project_id and run_name are required; other fields optional. In run_name, {date} is replaced
with the run date (DD/MM/YYYY). Full field list:
https://github.com/BitModern/testQualityCli/blob/master/src/UploadTestRunCommand.ts
"""

import logging
import os
import re

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, IS_AWS, NAMESPACE, ECSEnv

if IS_AWS:
    from radiant.dags.operators import ecs as operators
else:
    from radiant.dags.operators import k8s as operators

LOGGER = logging.getLogger(__name__)

TESTQUALITY_DEFAULT_API_URL = "https://api.testquality.com"

# Jira project ids whose tagged failing tests are treated as known errors
KNOWN_ERROR_JIRA_PROJECTS = ("SJRA",)
# Matches a known error tag in a test name, e.g. SJRA-1234 (case-insensitive)
KNOWN_ERROR_TAG = re.compile(rf"(?:{'|'.join(KNOWN_ERROR_JIRA_PROJECTS)})-\d+", re.IGNORECASE)


# we store dbt result files in a folder specific to the dag run
def _dbt_s3_uri(filename: str, run_id: str) -> str:
    bucket = os.getenv("RADIANT_DBT_S3_WORKSPACE")
    return f"s3://{bucket}/dbt-qa/{run_id}/{filename}"


def _download_from_s3(uri: str) -> bytes:
    from urllib.parse import urlparse

    import boto3

    parsed = urlparse(uri)
    obj = boto3.client("s3").get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return obj["Body"].read()


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
    @task(
        task_id="upload_to_testquality",
        task_display_name="Upload JUnit report to TestQuality",
    )
    def upload_to_testquality(junit_s3_uri: str) -> None:
        """Push the JUnit report produced by the container to TestQuality.

        Skips when the `skip_testquality_push` DAG param is enabled, or when the testquality_conn
        connection is absent.
        """
        import requests
        from airflow.exceptions import AirflowFailException, AirflowNotFoundException, AirflowSkipException
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context

        context = get_current_context()
        if context["params"]["skip_testquality_push"]:
            raise AirflowSkipException(
                "TestQuality upload disabled via the 'skip_testquality_push' DAG param — skipping."
            )

        try:
            conn = BaseHook.get_connection("testquality_conn")
        except AirflowNotFoundException:
            raise AirflowSkipException(
                "Connection 'testquality_conn' not configured — skipping TestQuality upload."
            ) from None

        junit_xml = _download_from_s3(junit_s3_uri)

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
    def check_results(run_results_s3_uri: str) -> None:
        """Fail the run if unexpected data tests failed.

        Failing tests tagged with a Jira ticket (SJRA-XXXX) are known errors: they are reported
        but do not fail the run.
        """
        import json
        from collections import Counter

        from airflow.exceptions import AirflowFailException

        run_results_json = json.loads(_download_from_s3(run_results_s3_uri))

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

    run_results_s3_uri = _dbt_s3_uri("run_results.json", "{{ run_id }}")
    junit_s3_uri = _dbt_s3_uri("junit.xml", "{{ run_id }}")

    if IS_AWS:
        run_dbt = operators.CheckDataIntegrity.get_run_dbt(run_results_s3_uri, junit_s3_uri, ECSEnv())
    else:
        run_dbt = operators.CheckDataIntegrity.get_run_dbt(run_results_s3_uri, junit_s3_uri)

    run_dbt >> [check_results(run_results_s3_uri), upload_to_testquality(junit_s3_uri)]


data_integrity_starrocks()
