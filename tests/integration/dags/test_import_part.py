import pytest

from tests.utils.dags import poll_dag_until_success


@pytest.mark.slow
def test_import_part_empty_sequencing_experiments(
    init_iceberg_tables,
    init_starrocks_tables,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
):
    _conf = '{"part":0}'
    dag_id = "radiant-import-part"
    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id, "--conf", _conf])
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)
