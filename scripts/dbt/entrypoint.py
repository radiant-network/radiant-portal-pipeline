#!/usr/bin/env python3
"""Container entrypoint for the data_qa dbt run (K8s pod / ECS task).

Parses the StarRocks Airflow connection into the SR_* env vars profiles.yml
expects, runs scripts/run_qa.sh verbatim, then uploads run_results.json and
junit.xml to the S3 URIs provided by the DAG.
"""
import json
import os
import subprocess
import sys
from urllib.parse import urlparse

import boto3

DATA_QA_DIR = "/opt/dbt/data_qa"


def export_starrocks_env(env):
    conn = json.loads(os.environ["AIRFLOW_CONN_STARROCKS_CONN"])
    env["SR_HOST"] = conn["host"]
    env["SR_PORT"] = str(conn.get("port") or 9030)
    env["SR_USER"] = conn.get("login") or "root"
    env["SR_PASSWORD"] = conn.get("password") or ""
    if conn.get("schema"):
        env["SR_SCHEMA"] = conn["schema"]


def upload(local_path, s3_uri):
    parsed = urlparse(s3_uri)
    boto3.client("s3").upload_file(local_path, parsed.netloc, parsed.path.lstrip("/"))
    print(f"Uploaded {local_path} -> {s3_uri}", flush=True)


def main():
    env = os.environ.copy()
    export_starrocks_env(env)

    # A non-zero exit means dbt could not run at all (e.g. no StarRocks connection):
    # there is nothing to upload, so fail the task.
    result = subprocess.run([f"{DATA_QA_DIR}/scripts/run_qa.sh"], env=env)
    if result.returncode != 0:
        print("dbt could not run (no run_results.json produced) — failing the task.", file=sys.stderr, flush=True)
        sys.exit(result.returncode)

    # Upload reports to s3 (overwrites if already present)
    upload(f"{DATA_QA_DIR}/target/run_results.json", os.environ["RUN_RESULTS_S3_URI"])
    upload(f"{DATA_QA_DIR}/reports/junit.xml", os.environ["JUNIT_S3_URI"])


if __name__ == "__main__":
    main()
