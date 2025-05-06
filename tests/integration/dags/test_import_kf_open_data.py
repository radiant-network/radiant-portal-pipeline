import logging

import pytest

from tests.utils.dags import poll_dag_until_success

LOGGER = logging.getLogger(__name__)


@pytest.mark.slow
def test_open_data_iceberg_tables(
    init_all_tables,
    starrocks_iceberg_catalog,
    setup_namespace,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
):
    _af_port = radiant_airflow_container.get_exposed_port(8080)
    _starrocks_port = starrocks_session.port

    dag_id = "radiant-import-open-data"
    _conf = f'{{"iceberg_catalog": "{starrocks_iceberg_catalog.name}", "iceberg_database": "{setup_namespace}"}}'

    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id, "--conf", _conf])

    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)

    with starrocks_session.cursor() as cursor:
        for _table in ["1000_genomes", "clinvar", "dbnsfp", "gnomad_genomes_v3", "spliceai", "topmed_bravo"]:
            cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_{_table}")
            response = cursor.fetchall()
            assert response[0][0] == 100, f"Table {_table} is empty"

            # TODO : Ajouter un check de la table des variants_lookup
