import pytest

from tests.utils.dags import poll_dag_until_success


@pytest.mark.slow
def test_import_part_empty_sequencing_experiments(
    init_all_tables,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
):
    _conf = {"part": 0}
    dag_id = "radiant-import-part"
    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id, "--conf", _conf])
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)


@pytest.mark.slow
def test_import_part_existing_sequencing_experiments(
    init_all_tables,
    radiant_airflow_container,
    starrocks_session,
    random_test_id,
):
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"""
            insert into test_{random_test_id}_sequencing_experiments (
               case_id,
               seq_id,
               part,
               analysis_type,
               sample_id,
               patient_id,
               vcf_filepath,
               sex,
               family_role,
               is_affected,
               created_at,
               updated_at,
               ingested_at
            ) VALUES (
              1, 1, 1, 'germline', 'SA0001', 'P14018', 's3+http://vcf/test.vcf.gz', 
              'F', 'proband', true, '2023-10-01 00:00:00', '2023-10-01 00:00:00', NULL
            );
        """)

    _conf = {"part": 0}
    dag_id = "radiant-import-part"
    radiant_airflow_container.exec(["airflow", "dags", "unpause", dag_id])
    radiant_airflow_container.exec(["airflow", "dags", "trigger", dag_id, "--conf", _conf])
    assert poll_dag_until_success(airflow_container=radiant_airflow_container, dag_id=dag_id, timeout=180)
