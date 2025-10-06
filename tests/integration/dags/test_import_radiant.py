import pytest

from tests.utils.dags import poll_dag_until_success, trigger_dag, unpause_dag


@pytest.mark.slow
def test_import_radiant(
    init_all_tables,
    starrocks_iceberg_catalog,
    starrocks_jdbc_catalog,
    radiant_airflow_container,
    starrocks_database,
    starrocks_session,
    random_test_id,
    clinical_snv_vcf,
    sample_exomiser_tsv,
    mapping_conf,
):
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")

    dag_id = "radiant-import"
    part_dag_id = "radiant-import-part"
    vcf_dag_id = "radiant-import-germline-snv-vcf"

    unpause_dag(radiant_airflow_container, vcf_dag_id)
    unpause_dag(radiant_airflow_container, part_dag_id)
    unpause_dag(radiant_airflow_container, dag_id)

    trigger_dag(radiant_airflow_container, dag_id, random_test_id, conf=mapping_conf)

    # Full import for test partitions takes around 5 minutes locally, so we set a longer timeout
    _timeout = 600
    assert poll_dag_until_success(
        airflow_container=radiant_airflow_container, dag_id=dag_id, run_id=random_test_id, timeout=_timeout
    )

    _table_count_mapping = {
        "staging_sequencing_experiment": [57],
        "germline__snv__consequence": [0],
        "germline__snv__consequence_filter": [0],
        "germline__snv__consequence_filter_partitioned": [0],
        "germline__snv__occurrence": [38, 40],
        "germline__snv__variant": [2],
        "germline__snv__variant_frequency": [1],
        "germline__snv__variant_partitioned": [2, 4],
        "germline__snv__staging_variant": [2],
        "germline__snv__staging_variant_frequency_part": [1, 4],
    }

    with starrocks_session.cursor() as cursor:
        for _table, counts in _table_count_mapping.items():
            cursor.execute(f"SELECT COUNT(1) FROM {_table}")
            response = cursor.fetchall()
            assert response[0][0] in counts, f"Table {_table} has count {response[0][0]}, expected {counts}"
