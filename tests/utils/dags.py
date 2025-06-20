import json
import time

import pandas as pd
import pyarrow as pa


def _exec_command(airflow_container, command: list[str]):
    if hasattr(airflow_container, "exec"):
        return airflow_container.exec(command)
    elif hasattr(airflow_container, "exec_run"):
        return airflow_container.exec_run(command)
    else:
        raise ValueError("The airflow_container does not support exec or exec_run methods.")


def unpause_dag(airflow_container, dag_id: str) -> None:
    """
    Unpauses the specified Airflow DAG.
    """
    _exec_command(airflow_container, ["airflow", "dags", "unpause", dag_id])
    print(f"DAG {dag_id} unpaused.")


def trigger_dag(airflow_container, dag_id: str, conf: str = None) -> None:
    """
    Triggers the specified Airflow DAG.
    """
    _cmd = ["airflow", "dags", "trigger", dag_id]
    if conf:
        _cmd.extend(["--conf", conf])
    _exec_command(airflow_container, _cmd)
    print(f"DAG {dag_id} triggered.")


def poll_dag_until_success(airflow_container, dag_id, timeout=120) -> bool:
    """
    Polls the Airflow container until the specified task is successful or the timeout is reached.
    """
    start_time = time.time()
    while True:
        _state = json.loads(
            _exec_command(airflow_container, ["airflow", "dags", "list-runs", "--dag-id", dag_id, "-o", "json"]).output
        )[0].get("state")
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
