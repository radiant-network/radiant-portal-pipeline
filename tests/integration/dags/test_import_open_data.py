import pytest

from radiant.tasks.data.radiant_tables import (
    RADIANT_DATABASE_ENV_KEY,
    RADIANT_ICEBERG_DATABASE_ENV_KEY,
)
from tests.utils.dags import poll_dag_until_success, trigger_dag, unpause_dag


@pytest.mark.slow
def test_import_open_data(
    init_all_tables,
    radiant_airflow_container,
    starrocks_database,
    starrocks_session,
    random_test_id,
    clinvar_rcv_summary_ndjson,
    starrocks_iceberg_catalog,
):
    dag_id = "radiant-import-open-data"
    unpause_dag(radiant_airflow_container, dag_id)
    dag_conf = {
        "raw_rcv_filepaths": ["s3://opendata/*.ndjson"],
        RADIANT_DATABASE_ENV_KEY: starrocks_database.database,
        RADIANT_ICEBERG_DATABASE_ENV_KEY: starrocks_iceberg_catalog.database,
    }
    trigger_dag(radiant_airflow_container, dag_id, random_test_id, conf=dag_conf)
    assert poll_dag_until_success(
        airflow_container=radiant_airflow_container, dag_id=dag_id, run_id=random_test_id, timeout=360
    )

    _table_count_mapping = {
        "raw_clinvar_rcv_summary": [5],
        "clinvar_rcv_summary": [5],
    }

    with starrocks_session.cursor() as cursor:
        for _table, counts in _table_count_mapping.items():
            cursor.execute(f"SELECT COUNT(1) FROM {_table}")
            response = cursor.fetchall()
            assert response[0][0] in counts, f"Table {_table} has count {response[0][0]}, expected {counts}"
