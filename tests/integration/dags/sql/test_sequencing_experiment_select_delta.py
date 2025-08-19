import os

import jinja2
import pandas as pd
import psycopg2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import (
    CLINICAL_CATALOG_ENV_KEY,
    CLINICAL_DATABASE_ENV_KEY,
    RADIANT_DATABASE_ENV_KEY,
)

_SQL_DIR = os.path.join(DAGS_DIR, "sql")
_RADIANT_SQL_PATH = os.path.join(_SQL_DIR, "radiant")


@pytest.fixture(scope="session")
def sequencing_delta_columns():
    yield [
        "case_id",
        "seq_id",
        "task_id",
        "analysis_type",
        "aliquot",
        "patient_id",
        "experimental_strategy",
        "request_id",
        "request_priority",
        "vcf_filepath",
        "exomiser_filepaths",
        "sex",
        "family_role",
        "affected_status",
        "created_at",
        "updated_at",
        "patient_part",
        "case_part",
        "max_part",
        "max_count",
    ]


def _run_radiant_sql(starrocks_session, starrocks_jdbc_catalog, starrocks_container, sql_file):
    """
    Helper function to run a SQL file against the StarRocks session.
    """
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    with open(sql_file) as f:
        template = jinja2.Template(f.read())
        conf = {
            RADIANT_DATABASE_ENV_KEY: starrocks_container.database,
            CLINICAL_CATALOG_ENV_KEY: starrocks_jdbc_catalog.catalog,
            CLINICAL_DATABASE_ENV_KEY: starrocks_jdbc_catalog.database,
        }
        sql = template.render(params=get_radiant_mapping(conf=conf))
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


@pytest.fixture(scope="session")
def sequencing_experiment_tables(starrocks_session, starrocks_jdbc_catalog, starrocks_database):
    """
    Fixture to create a temporary sequencing_experiment table for testing.
    """
    _run_radiant_sql(
        starrocks_session,
        starrocks_jdbc_catalog,
        starrocks_database,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "staging_sequencing_experiment_create_table.sql"),
    )
    _run_radiant_sql(
        starrocks_session,
        starrocks_jdbc_catalog,
        starrocks_database,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "staging_external_sequencing_experiment_create_table.sql"),
    )
    _run_radiant_sql(
        starrocks_session,
        starrocks_jdbc_catalog,
        starrocks_database,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "staging_sequencing_experiment_delta_create_table.sql"),
    )
    yield


def test_sequencing_experiment_empty(starrocks_session, sequencing_experiment_tables, sequencing_delta_columns):
    """
    Test the case where we "start from scratch" with nothing in the sequencing_experiment table.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    assert results is not None, "Results should not be None"
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 56


def test_sequencing_experiment_no_delta(starrocks_session, sequencing_experiment_tables, sequencing_delta_columns):
    """
    Test the case where there's no delta, i.e., the sequencing_experiment table is already fully populated.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT case_id,
                              seq_id,
                              task_id,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_id,
                              request_priority,
                              vcf_filepath,
                              exomiser_filepaths,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at
                       FROM staging_external_sequencing_experiment
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 0


def test_sequencing_experiment_existing_wgs_case_partition(
    starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when there's an existing WGS case partition existing.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT case_id,
                              seq_id,
                              task_id,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_id,
                              request_priority,
                              vcf_filepath,
                              exomiser_filepaths,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at
                       FROM staging_external_sequencing_experiment
                       WHERE case_id = 1
                         AND seq_id = 1
                         AND task_id = 1
                       """)
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT case_id,
                              seq_id,
                              task_id,
                              1                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_id,
                              request_priority,
                              vcf_filepath,
                              exomiser_filepaths,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at
                       FROM staging_external_sequencing_experiment
                       WHERE case_id = 2
                         AND seq_id = 4
                         AND task_id = 4
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 55  # 57 total - 2 imported experiments


def test_sequencing_experiment_existing_wxs_case_partition(
    starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when there's an existing WXS case partition existing.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT case_id,
                              seq_id,
                              task_id,
                              65537                 AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_id,
                              request_priority,
                              vcf_filepath,
                              exomiser_filepaths,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at
                       FROM staging_external_sequencing_experiment
                       WHERE case_id = 1
                         AND seq_id = 62
                         AND task_id = 62
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 56  # 57 total - 1 imported wxs experiments


def test_sequencing_experiment_with_recently_updated_case(
    postgres_instance, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when an existing sequencing experiment gets an update
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("""
                       INSERT INTO staging_sequencing_experiment
                       SELECT case_id,
                              seq_id,
                              task_id,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              request_id,
                              request_priority,
                              vcf_filepath,
                              exomiser_filepaths,
                              sex,
                              family_role,
                              affected_status,
                              created_at,
                              updated_at,
                              '1970-01-01 00:00:00' AS ingested_at
                       FROM staging_external_sequencing_experiment
                       WHERE case_id = 1
                         AND seq_id = 1
                         AND task_id = 1
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 56  # 57 total - 1 imported wxs experiments

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
                          UPDATE sequencing_experiment
                          SET updated_on = date_trunc('day', NOW())
                          WHERE case_id = 1
                            AND id = 1
                          """)
        pg_conn.commit()

    with starrocks_session.cursor() as cursor:
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    # Should capture the updated experiment
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 57
