import os

import jinja2
import pandas as pd
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
        "part",
        "analysis_type",
        "sample_id",
        "patient_id",
        "experimental_strategy",
        "vcf_filepath",
        "sex",
        "family_role",
        "affected_status",
        "created_at",
        "updated_at",
    ]


def _run_radiant_sql(starrocks_session, sql_file):
    """
    Helper function to run a SQL file against the StarRocks session.
    """
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    with open(sql_file) as f:
        template = jinja2.Template(f.read())
        sql = template.render(params=get_radiant_mapping())

    with starrocks_session.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


@pytest.fixture(scope="session")
def sequencing_experiment_tables(starrocks_session, starrocks_jdbc_catalog):
    """
    Fixture to create a temporary sequencing_experiment table for testing.
    """
    os.environ["RADIANT_CLINICAL_CATALOG"] = starrocks_jdbc_catalog
    os.environ["RADIANT_STARROCKS_UDF_JAR_PATH"] = (
        "http://host.docker.internal:8000/radiant-starrocks-udf-1.0.1-jar-with-dependencies.jar"
    )
    _run_radiant_sql(
        starrocks_session, sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "sequencing_experiment_create_table.sql")
    )
    _run_radiant_sql(
        starrocks_session,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "staging_sequencing_experiment_create_table.sql"),
    )
    _run_radiant_sql(
        starrocks_session,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "get_sequencing_experiment_partition_udf.sql"),
    )
    _run_radiant_sql(
        starrocks_session,
        sql_file=os.path.join(_RADIANT_SQL_PATH, "init", "init_sequencing_experiment_partition_udf.sql"),
    )
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE sequencing_experiment;")
    yield


@pytest.mark.manual
def test_sequencing_experiment_empty(starrocks_session, sequencing_experiment_tables, sequencing_delta_columns):
    """
    Test the case where we "start from scratch" with nothing in the sequencing_experiment table.
    """
    results = _run_radiant_sql(
        starrocks_session, sql_file=os.path.join(_RADIANT_SQL_PATH, "sequencing_experiment_select_delta.sql")
    )
    assert results is not None, "Results should not be None"
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 64
    assert result_df["part"].unique() == [0, 65536]


@pytest.mark.manual
def test_sequencing_experiment_full(starrocks_session, sequencing_experiment_tables, sequencing_delta_columns):
    """
    Test the case where there's no delta, i.e., the sequencing_experiment table is already fully populated.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("""
        INSERT INTO sequencing_experiment 
        SELECT case_id, seq_id, task_id, 0 AS part, analysis_type, sample_id, patient_id, experimental_strategy,
            vcf_filepath, sex, family_role, affected_status, 
            created_at, updated_at, '1970-01-01 00:00:00' AS ingested_at 
        FROM staging_sequencing_experiment
        """)
    results = _run_radiant_sql(
        starrocks_session, sql_file=os.path.join(_RADIANT_SQL_PATH, "sequencing_experiment_select_delta.sql")
    )
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 0


@pytest.mark.manual
def test_sequencing_experiment_existing_wgs_case_partition(
    starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when there's an existing WGS case partition existing.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("""
            INSERT INTO sequencing_experiment 
            SELECT case_id, seq_id, task_id, 0 AS part, analysis_type, sample_id, patient_id, experimental_strategy,
                vcf_filepath, sex, family_role, affected_status, 
                created_at, updated_at, '1970-01-01 00:00:00' AS ingested_at 
            FROM staging_sequencing_experiment
            WHERE case_id = 1 AND seq_id = 1 AND task_id = 1
        """)
        cursor.execute("""
            INSERT INTO sequencing_experiment 
            SELECT case_id, seq_id, task_id, 1 AS part, analysis_type, sample_id, patient_id, experimental_strategy,
                vcf_filepath, sex, family_role, affected_status, 
                created_at, updated_at, '1970-01-01 00:00:00' AS ingested_at 
            FROM staging_sequencing_experiment
            WHERE case_id = 2 AND seq_id = 4 AND task_id = 4
        """)
    results = _run_radiant_sql(
        starrocks_session, sql_file=os.path.join(_RADIANT_SQL_PATH, "sequencing_experiment_select_delta.sql")
    )
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 62  # 64 total - 2 imported experiments
    assert sorted(result_df["part"].unique().tolist()) == [0, 1, 65536]
    assert result_df[result_df["part"] == 0]["seq_id"].tolist() == [2, 3]
    assert result_df[result_df["part"] == 65536]["seq_id"].tolist() == [62, 63, 64]


@pytest.mark.manual
def test_sequencing_experiment_existing_wxs_case_partition(
    starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when there's an existing WXS case partition existing.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("""
            INSERT INTO sequencing_experiment 
            SELECT case_id, seq_id, task_id, 65537 AS part, analysis_type, sample_id, patient_id, 
                experimental_strategy, vcf_filepath, sex, family_role, affected_status, 
                created_at, updated_at, '1970-01-01 00:00:00' AS ingested_at 
            FROM staging_sequencing_experiment
            WHERE case_id = 1 AND seq_id = 62 AND task_id = 62
        """)
    results = _run_radiant_sql(
        starrocks_session, sql_file=os.path.join(_RADIANT_SQL_PATH, "sequencing_experiment_select_delta.sql")
    )
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 63  # 63 total - 1 imported wxs experiments
    assert sorted(result_df["part"].unique().tolist()) == [0, 65537]
    assert result_df[result_df["part"] == 65537]["seq_id"].tolist() == [63, 64]


@pytest.mark.manual
def test_sequencing_experiment_partition_full(
    starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when a partition is full
    """
    _values = ",".join(
        [
            f"({i}, {i}, {i}, 0, 'germline', {i}, {i}, 'wgs', 's3+http://vcf/F1000.vcf.gz', 'male', "
            f"'probant', 'affected', '2025-01-01 00:00:00', '2025-01-01 00:00:00', '2025-01-02 00:00:00')"
            for i in range(1000, 1100)
        ]
    )
    _sql = f"""
    INSERT INTO sequencing_experiment
    VALUES {_values}
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute(_sql)
    results = _run_radiant_sql(
        starrocks_session, sql_file=os.path.join(_RADIANT_SQL_PATH, "sequencing_experiment_select_delta.sql")
    )
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 64
    assert sorted(result_df["part"].unique().tolist()) == [1, 65536]  # Partition 0 is already full for WGS


@pytest.mark.manual
def test_sequencing_experiment_partition_full_but_matching_case_id(
    starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """
    Test computing the delta when a partition is full
    """
    _values = ",".join(
        [
            f"(1, {i}, {i}, 0, 'germline', {i}, {i}, 'wgs', 's3+http://vcf/F1000.vcf.gz', 'male', "
            f"'probant', 'affected', '2025-01-01 00:00:00', '2025-01-01 00:00:00', '2025-01-02 00:00:00')"
            for i in range(1000, 1100)
        ]
    )
    _sql = f"""
    INSERT INTO sequencing_experiment
    VALUES {_values}
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute(_sql)
    results = _run_radiant_sql(
        starrocks_session, sql_file=os.path.join(_RADIANT_SQL_PATH, "sequencing_experiment_select_delta.sql")
    )
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 64
    assert sorted(result_df["part"].unique().tolist()) == [
        0,
        1,
        65536,
    ]  # Partition 0 is full, but matching case has priority
