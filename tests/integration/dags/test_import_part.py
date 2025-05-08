import pytest

from tests.utils.dags import poll_dag_until_success


@pytest.mark.slow
def test_import_part_with_valid_sequencing_experiments(
    init_iceberg_tables,
    init_starrocks_tables,
    starrocks_iceberg_catalog,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
    s3_fs,
    indexed_vcfs,
):
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"""
            insert into test_{random_test_id}.test_{random_test_id}_sequencing_experiments (
                case_id, seq_id, part, analysis_type, sample_id, patient_id, vcf_filepath, sex, family_role,
                is_affected, created_at, updated_at, ingested_at)
            VALUES (
                0, 0, 0, 'germline', 'SA0001', 'P14018', '{indexed_vcfs["test.vcf"]}', 
                'F', 'proband', true, '2023-10-01 00:00:00', '2023-10-01 00:00:00', NULL
            );
        """)

    _conf = '{"part":0}'
    dag_id = "radiant-import-part"
    vcf_dag_id = "radiant-import-vcf"

    radiant_airflow_container.exec(["airflow", "dags", "unpause", vcf_dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id, "--conf", _conf])
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)

    _table_count_mapping = {
        "sequencing_experiments": 1,
        "consequences": 0,
        "consequences_filter": 0,
        "consequences_filter_partitioned": 0,
        "occurrences": 2,
        "variants": 2,
        "variants_frequencies": 2,
        "variants_partitioned": 2,
        "variants_lookup": 2,
        "staging_variants": 2,
        "staging_variants_frequencies_part": 2,
    }

    with starrocks_session.cursor() as cursor:
        for _table, count in _table_count_mapping.items():
            cursor.execute(f"SELECT COUNT(1) FROM test_{random_test_id}_{_table}")
            response = cursor.fetchall()
            assert response[0][0] == count, f"Table {_table} has count {response[0][0]}, expected {count}"
