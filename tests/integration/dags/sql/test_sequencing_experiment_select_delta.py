import os

import jinja2
import pandas as pd
import psycopg2
import pytest

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")
_RADIANT_SQL_PATH = os.path.join(_SQL_DIR, "radiant")


@pytest.fixture(scope="session")
def sequencing_delta_columns():
    yield [
        "seq_id",
        "task_id",
        "task_type",
        "analysis_type",
        "aliquot",
        "patient_id",
        "experimental_strategy",
        "request_priority",
        "vcf_filepath",
        "csv_filepath",
        "exomiser_filepath",
        "sex",
        "family_role",
        "affected_status",
        "created_at",
        "updated_at",
        "patient_part",
        "task_part",
        "max_part",
        "max_count",
    ]


def _run_radiant_sql(starrocks_session, radiant_mapping, sql_file):
    """
    Helper function to run a SQL file against the StarRocks session.
    """

    with open(sql_file) as f:
        template = jinja2.Template(f.read())
        sql = template.render(mapping=radiant_mapping)
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


@pytest.fixture(scope="session")
def sequencing_experiment_tables(starrocks_session, radiant_mapping):
    """
    Fixture to create a temporary sequencing_experiment table for testing.
    """
    _run_radiant_sql(
        starrocks_session,
        radiant_mapping,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "staging_sequencing_experiment_create_table.sql"),
    )
    _run_radiant_sql(
        starrocks_session,
        radiant_mapping,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "staging_external_sequencing_experiment_create_table.sql"),
    )
    _run_radiant_sql(
        starrocks_session,
        radiant_mapping,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "staging_sequencing_experiment_delta_create_table.sql"),
    )
    yield


def test_sequencing_experiment_empty(
    postgres_clinical_seeds, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test the case where we "start from scratch" with nothing in the sequencing_experiment table.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    assert results is not None, "Results should not be None"
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 10


def test_sequencing_experiment_no_delta(
    postgres_clinical_seeds, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test the case where there's no delta, i.e., the sequencing_experiment table is already fully populated.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT seq_id,
                              task_id,
                              task_type,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at,
                              false as deleted
                       FROM staging_external_sequencing_experiment
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 0


def test_sequencing_experiment_existing_wgs_task_partition(
    postgres_clinical_seeds, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when there's an existing WGS task partition existing.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT seq_id,
                              task_id,
                              task_type,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at,
                              false as deleted
                       FROM staging_external_sequencing_experiment
                       WHERE seq_id = 22
                         AND task_id = 66
                       """)
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT seq_id,
                              task_id,
                              task_type,
                              1                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at,
                              false as deleted
                       FROM staging_external_sequencing_experiment
                       WHERE seq_id = 4
                         AND task_id = 4
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 9  # 4 total - 1 imported experiments


def test_sequencing_experiment_with_recently_updated_task(
    postgres_instance, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when an existing sequencing experiment gets an update
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT seq_id,
                              task_id,
                              task_type,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at,
                              false as deleted
                       FROM staging_external_sequencing_experiment
                       WHERE seq_id = 22
                         AND task_id = 66
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 9

    with (
        psycopg2.connect(
            host="localhost",
            port=postgres_instance.port,
            database=postgres_instance.radiant_db,
            user=postgres_instance.user,
            password=postgres_instance.password,
        ) as pg_conn,
        pg_conn.cursor() as pg_cursor,
    ):
        pg_cursor.execute(f"SET search_path TO {postgres_instance.radiant_db_schema};")
        pg_cursor.execute("""
                          UPDATE cases
                          SET updated_on = NOW()
                          WHERE id = 8
                          """)
        pg_conn.commit()

    with starrocks_session.cursor() as cursor:
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    # Should capture the updated experiment
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 10
