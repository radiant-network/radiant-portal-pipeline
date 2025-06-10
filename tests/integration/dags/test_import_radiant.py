import pytest

from tests.utils.dags import poll_dag_until_success


@pytest.mark.slow
def test_import_radiant(
    init_all_tables,
    starrocks_iceberg_catalog,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
    indexed_vcfs,
    clinical_vcf,
):
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE test_{random_test_id}.test_{random_test_id}_staging_sequencing_experiment;")

    dag_id = "radiant-import"
    part_dag_id = "radiant-import-part"
    vcf_dag_id = "radiant-import-vcf"

    radiant_airflow_container.exec(["airflow", "dags", "unpause", vcf_dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "unpause", part_dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id])

    # Full import for test partitions takes around 5 minutes locally, so we set a longer timeout
    _timeout = 600
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=_timeout)

    _table_count_mapping = {
        "staging_sequencing_experiment": 64,
        "consequence": 0,
        "consequence_filter": 0,
        "consequence_filter_partitioned": 0,
        "occurrence": 44,
        "variant": 2,
        "variant_frequency": 2,
        "variant_partitioned": 4,
        "staging_variant": 2,
        "staging_variant_frequency_part": 4,
    }

    with starrocks_session.cursor() as cursor:
        for _table, count in _table_count_mapping.items():
            cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_{_table}")
            response = cursor.fetchall()
            assert response[0][0] == count, f"Table {_table} has count {response[0][0]}, expected {count}"
