import pytest

from tests.utils.dags import poll_dag_until_success, trigger_dag, unpause_dag


@pytest.mark.slow
def test_open_data_iceberg_tables(
    init_all_tables,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
):
    dag_id = "radiant-import-open-data"
    unpause_dag(radiant_airflow_container, dag_id)
    trigger_dag(radiant_airflow_container, dag_id)
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)
