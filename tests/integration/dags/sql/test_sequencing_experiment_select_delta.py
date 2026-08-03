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
        "case_id",
        "seq_id",
        "task_id",
        "task_type",
        "analysis_type",
        "aliquot",
        "patient_id",
        "experimental_strategy",
        "tenant_code",
        "request_priority",
        "vcf_filepath",
        "cnv_vcf_filepath",
        "exomiser_filepath",
        "sex",
        "family_id",
        "family_role",
        "affected_status",
        "histology_type",
        "created_at",
        "updated_at",
        "patient_part",
        "seq_part",
        "case_part",
        "family_part",
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
    assert len(result_df) == 14


def test_sequencing_experiment_delta_carries_tumor_only_tasks(
    postgres_clinical_seeds, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """Somatic tasks with a single tumoral aliquot reach the delta, in both shapes that exist.

    Nothing upstream flags which analysis is which — a task is tumor-only iff it has exactly one
    'tumoral' aliquot and no 'normal' one. So the view must emit one row per (case, seq, task) and
    leave the aliquot count to say it. The seeds cover all three shapes:

    - task 67: tumor-normal, case 22, experiments 62 (tumoral) + 63 (normal)
    - task 68: tumor-only on the SAME tumor sample as 67 (experiment 62) — so case 22 is
      simultaneously both, which a per-case discriminator could not express
    - task 69: tumor-only on its own case 23 / experiment 64, a tumor sample with no normal at all

    Tasks 68 and 69 belong to different patients, so the tumor-only cohort has two members and its
    frequency denominators are not degenerate.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    somatic = pd.DataFrame(results, columns=sequencing_delta_columns).query("analysis_type == 'somatic'")

    assert {int(task_id): sorted(rows["histology_type"]) for task_id, rows in somatic.groupby("task_id")} == {
        67: ["normal", "tumoral"],
        68: ["tumoral"],
        69: ["tumoral"],
    }

    # Each tumor-only task points at its own single-sample VCF, never the paired one.
    tumor_only = {
        int(row["task_id"]): (int(row["seq_id"]), row["aliquot"], row["patient_id"], row["vcf_filepath"])
        for _, row in somatic.query("task_id in [68, 69]").iterrows()
    }
    assert tumor_only[68][:2] == (62, "TCR002361_SRX1091647-T")
    assert tumor_only[68][3].endswith("variants.SRX1091647-T.snv.vep.vcf.gz")
    assert tumor_only[69][:2] == (64, "TCRBOA6_SRX1166091-T")
    assert tumor_only[69][3].endswith("variants.SRX1166091-T.snv.vep.vcf.gz")

    # Task 68 shares experiment 62 with the tumor-normal task; task 69 is a distinct patient.
    assert 62 in set(somatic.query("task_id == 67")["seq_id"])
    assert tumor_only[68][2] != tumor_only[69][2]

    # Every somatic experiment here is wgs, so the tumor-only cohort lands in the wgs buckets.
    assert set(somatic["experimental_strategy"]) == {"wgs"}


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
                       SELECT case_id,
                              seq_id,
                              task_id,
                              task_type,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              tenant_code,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_id,
                              family_role,
                              affected_status,
                              histology_type,
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
                       SELECT case_id,
                              seq_id,
                              task_id,
                              task_type,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              tenant_code,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_id,
                              family_role,
                              affected_status,
                              histology_type,
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
                       SELECT case_id,
                              seq_id,
                              task_id,
                              task_type,
                              1                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              tenant_code,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_id,
                              family_role,
                              affected_status,
                              histology_type,
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
    assert len(result_df) == 13


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
                       SELECT case_id,
                              seq_id,
                              task_id,
                              task_type,
                              0                     AS part,
                              analysis_type,
                              aliquot,
                              patient_id,
                              experimental_strategy,
                              tenant_code,
                              request_priority,
                              vcf_filepath,
                              cnv_vcf_filepath,
                              exomiser_filepath,
                              sex,
                              family_id,
                              family_role,
                              affected_status,
                              histology_type,
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
    assert len(result_df) == 13

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
    assert len(result_df) == 14
