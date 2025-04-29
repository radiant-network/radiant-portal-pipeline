import time

from tests.utils.dag_run import poll_dag_until_success


def test_open_data_iceberg_tables(
    init_iceberg_tables, starrocks_iceberg_catalog, setup_namespace, radiant_airflow_container, starrocks_session
):
    _af_port = radiant_airflow_container.get_exposed_port(8080)
    _sr_port = starrocks_session.port

    hashes_dag_id = "radiant-import-kf-hashes"
    dag_id = "radiant-import-kf-open-data"
    _conf = f'{{"iceberg_catalog": "{starrocks_iceberg_catalog.name}", "iceberg_database": "{setup_namespace}"}}'

    time.sleep(30)

    # Hashes are required to be imported first, but standalone airflow doesn't support
    # running nested dags, therefore we need to trigger them sequentially manually
    radiant_airflow_container.exec(["airflow", "dags", "unpause", hashes_dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", hashes_dag_id, "--conf", _conf])

    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=hashes_dag_id, timeout=120)

    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id, "--conf", _conf])

    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=120)

    with starrocks_session.cursor() as cursor:
        for _table in [
            "1000_genomes",
            "clinvar",
            "dbnsfp",
            "gnomad_genomes_v3",
            "spliceai",
            "topmed_bravo",
        ]:
            cursor.execute(f"SELECT COUNT(1) FROM {_table}")
            response = cursor.fetchall()
            assert response[0][0] == 100, f"Table {_table} is empty"
