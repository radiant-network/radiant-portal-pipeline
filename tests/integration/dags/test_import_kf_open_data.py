import pytest

from tests.utils.dags import poll_dag_until_success


@pytest.mark.slow
def test_open_data_iceberg_tables(
    init_all_tables,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
):
    dag_id = "radiant-import-open-data"
    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id])

    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)

    with starrocks_session.cursor() as cursor:
        for _table in [
            "1000_genomes",
            "clinvar",
            "dbnsfp",
            "gnomad_genomes_v3",
            "gnomad_constraints",
            "spliceai",
            "topmed_bravo",
        ]:
            cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_{_table}")
            response = cursor.fetchall()
            assert response[0][0] == 100, f"Table {_table} is empty"

        cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_variants_lookup")
        response = cursor.fetchall()
        assert response[0][0] > 0, "Table variants_lookup is empty"
