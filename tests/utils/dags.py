import json
import time

import pandas as pd
import pyarrow as pa
import requests


def unpause_dag(airflow_container, dag_id: str) -> None:
    unpause_url = f"{airflow_container.endpoint}/api/v1/dags/{dag_id}"
    response = requests.patch(
        unpause_url,
        auth=(airflow_container.username, airflow_container.password),
        headers={"Content-Type": "application/json"},
        json={"is_paused": False},
    )

    # 2. Check response
    if response.status_code == 200:
        print(f"DAG '{dag_id}' is now unpaused.")
    else:
        raise RuntimeError(f"Failed to unpause DAG: {response.status_code} {response.text}")


def trigger_dag(airflow_container, dag_id: str, run_id: str, conf: dict = None) -> str:
    trigger_url = f"{airflow_container.endpoint}/api/v1/dags/{dag_id}/dagRuns"

    response = requests.post(
        trigger_url,
        auth=(airflow_container.username, airflow_container.password),
        json={"dag_run_id": run_id, "conf": conf or {}},
    )

    if response.status_code != 200:
        raise RuntimeError(f"Failed to trigger DAG: {response.status_code} {response.text}")
    return run_id


def poll_dag_until_success(airflow_container, dag_id, run_id, timeout=120) -> bool:
    """
    Polls the Airflow container until the specified task is successful, or the timeout is reached.
    """
    start_time = time.time()
    status_url = f"{airflow_container.endpoint}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
    while True:
        resp = requests.get(status_url, auth=(airflow_container.username, airflow_container.password))
        _state = resp.json()["state"]
        if _state == "success":
            print(f"Task {dag_id} succeeded.")
            return True

        elif _state == "failed":
            print(f"Task {dag_id} failed.")
            return False

        if time.time() - start_time > timeout:
            raise TimeoutError(f"DAG {dag_id} did not succeed within {timeout} seconds")

        time.sleep(5)


def get_pyarrow_table_from_csv(
    csv_path, sep: str | None, json_fields: list[str] | None = None, is_clinvar: bool = False
) -> pa.Table:
    """
    Get the pyarrow schema from a CSV file.
    """
    df = pd.read_csv(csv_path, sep=sep)

    if is_clinvar:
        df = df.map(lambda x: [""] if pd.isna(x) else x)

    if json_fields:
        for field in json_fields:
            if field in df.columns:
                df[field] = df[field].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).all():
            df[col] = pa.array(df[col].tolist(), type=pa.list_(pa.string()))

    return pa.Table.from_pandas(df)
