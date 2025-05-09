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

    _table_counts = {
        "1000_genomes": 100,
        "clinvar": 100,
        "dbnsfp": 100,
        "gnomad_genomes_v3": 100,
        "gnomad_constraints": 100,
        "spliceai": 100,
        "topmed_bravo": 100,
        "omim_gene_set": 167,
    }

    _table_counts = {
        "1000_genomes": 100,
        "clinvar": 100,
        "dbnsfp": 100,
        "gnomad_genomes_v3": 100,
        "gnomad_constraints": 100,
        "spliceai": 100,
        "topmed_bravo": 100,
        "omim_gene_panel": 167,
        "cosmic_gene_panel": 146,
        "ddd_gene_panel": 104,
        "hpo_gene_panel": 100,
        "orphanet_gene_panel": 94,
    }

    with starrocks_session.cursor() as cursor:
        for _table, count in _table_counts.items():
            cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_{_table}")
            response = cursor.fetchall()
            assert response[0][0] == count, f"Table {_table} is empty"

        cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_variants_lookup")
        response = cursor.fetchall()
        assert response[0][0] > 0, "Table variants_lookup is empty"
