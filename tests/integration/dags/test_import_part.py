import pytest

from tests.utils.dags import poll_dag_until_success


@pytest.mark.slow
def test_import_part_with_valid_sequencing_experiments(
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
        cursor.execute(f"""
            insert into test_{random_test_id}.test_{random_test_id}_staging_sequencing_experiment (
                case_id, seq_id, task_id, part, analysis_type, aliquot, patient_id, experimental_strategy, 
                request_id, request_priority, vcf_filepath, sex, family_role, affected_status, created_at, 
                updated_at, ingested_at)
            VALUES (
                1, 1, 1, 0, 'germline', 1, 3, 'wgs', 1, 'routine', 's3+http://vcf/FI0037862.S14747.vcf.gz', 
                'male', 'proband', 'affected', '2025-10-01 00:00:00', '2025-10-01 00:00:00', NULL
            );
        """)

    _conf = '{"part":0}'
    dag_id = "radiant-import-part"
    vcf_dag_id = "radiant-import-vcf"

    radiant_airflow_container.exec(["airflow", "dags", "unpause", vcf_dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id, "--conf", _conf])
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=120)

    _table_count_mapping = {
        "staging_sequencing_experiment": 1,
        "consequence": 0,
        "consequence_filter": 0,
        "consequence_filter_partitioned": 0,
        "occurrence": 2,
        "variant": 2,
        "variant_frequency": 2,
        "variant_partitioned": 2,
        "staging_variant": 2,
        "staging_variant_frequency_part": 2,
    }

    with starrocks_session.cursor() as cursor:
        for _table, count in _table_count_mapping.items():
            cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_{_table}")
            response = cursor.fetchall()
            assert response[0][0] == count, f"Table {_table} has count {response[0][0]}, expected {count}"
