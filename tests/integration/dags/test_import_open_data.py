import pytest

from tests.utils.dags import poll_dag_until_success, trigger_dag, unpause_dag


@pytest.mark.slow
def test_import_open_data(
    init_all_tables,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
    clinvar_rcv_summary_ndjson,
):
    dag_id = "radiant-import-open-data"
    unpause_dag(radiant_airflow_container, dag_id)
    trigger_dag(radiant_airflow_container, dag_id, conf='{"raw_rcv_filepaths":["s3://opendata/*.ndjson"]}')
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)

    _table_count_mapping = {
        "raw_clinvar_rcv_summary": [5],
        "clinvar_rcv_summary": [5],
    }

    with starrocks_session.cursor() as cursor:
        for _table, counts in _table_count_mapping.items():
            cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}.{_table}")
            response = cursor.fetchall()
            assert response[0][0] in counts, f"Table {_table} has count {response[0][0]}, expected {counts}"
