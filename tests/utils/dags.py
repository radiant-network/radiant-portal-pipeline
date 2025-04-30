import json
import time

import pandas as pd
import pyarrow as pa


def poll_dag_until_success(airflow_container, dag_id, timeout=120) -> bool:
    """
    Polls the Airflow container until the specified task is successful or the timeout is reached.
    """
    start_time = time.time()
    while True:
        _state = airflow_container.exec(["airflow", "dags", "list-runs", "--dag-id", dag_id])
        if b"success" in _state.output:
            print(f"Task {dag_id} succeeded.")
            return True

        elif b"failed" in _state.output:
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
