import time


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
