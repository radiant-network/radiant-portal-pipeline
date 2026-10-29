import os

import jinja2
import pandas as pd
import psycopg2
import pytest

from radiant.dags import DAGS_DIR
from tests.integration.fixtures_common import execute_clinical

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
    # Whole-seed baseline: every (case, seq, task) the view emits. It moves whenever seeds.sql gains a
    # document the view's WHERE clause accepts -- an SNV/CNV VCF on an annotation or calling task, or an
    # exomiser tsv on a germline annotation task. SJRA-1928 took it 15 -> 18 by giving the case-16 trio
    # (alignment tasks 44/45/46) their gcnv VCFs. Moving the exomiser tsvs of the legacy `exomiser` tasks 64
    # and 66 onto their annotation tasks 63 and 65 took it 18 -> 16.
    assert len(result_df) == 16


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
    # Task 70 is the tumor-only *calling* step behind task 69, covered by its own test below.
    annotations = somatic.query("task_type == 'radiant_somatic_annotation'")

    assert {int(task_id): sorted(rows["histology_type"]) for task_id, rows in annotations.groupby("task_id")} == {
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


def test_sequencing_experiment_delta_gates_somatic_cnv_documents(
    postgres_clinical_seeds, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """A somatic CNV VCF is discovered by task type + document data type, never by filename.

    Task 70 is the `tumor_only_variant_calling` step behind the tumor-only annotation task 69, and it
    registers three output documents: the `scnv` CNV VCF, its index, and a raw un-annotated `ssnv`
    SNV VCF. Only the CNV VCF may surface — the raw SNV file must not reach `vcf_filepath`, which
    only ever holds the annotated VCF of a `radiant_somatic_annotation` task. Without the
    `data_type_code = 'scnv'` gate in the view's WHERE clause, it would, giving somatic SNV a second
    ingestion route it must not have.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    delta = pd.DataFrame(results, columns=sequencing_delta_columns)
    calling = delta.query("task_id == 70")

    # One row: the task's single tumoral experiment, carrying the CNV VCF and nothing else.
    assert len(calling) == 1
    row = calling.iloc[0]
    assert row["task_type"] == "tumor_only_variant_calling"
    assert (row["seq_id"], row["aliquot"], row["histology_type"]) == (64, "TCRBOA6_SRX1166091-T", "tumoral")
    assert row["cnv_vcf_filepath"].endswith("TCRBOA6_SRX1166091-T.cnv.vcf.gz")
    # pd.isna, not `is None`: pandas renders a SQL NULL as either None or NaN depending on what else
    # the column holds, and a leak would show up as a URL string either way.
    assert pd.isna(row["vcf_filepath"]), "the raw un-annotated ssnv VCF leaked into vcf_filepath"

    # The annotation task on the same experiment still points at its own annotated VCF.
    annotation = delta.query("task_id == 69").iloc[0]
    assert annotation["vcf_filepath"].endswith("variants.SRX1166091-T.snv.vep.vcf.gz")
    assert pd.isna(annotation["cnv_vcf_filepath"])


def _delta(starrocks_session, sequencing_delta_columns):
    with starrocks_session.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE staging_sequencing_experiment;")
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        return pd.DataFrame(cursor.fetchall(), columns=sequencing_delta_columns)


def test_sequencing_experiment_delta_sets_exomiser_file_on_the_proband_row_only(
    postgres_clinical_seeds, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """The Exomiser tsv is an output of the germline annotation task, which holds the aliquots of
    every family member. Only the proband's row carries it: the load tags the Exomiser rows with that seq_id.

    Task 63 annotates the case-1 trio (experiments 1, 2 and 3); task 65 the solo case 8 (experiment 22).
    """
    delta = _delta(starrocks_session, sequencing_delta_columns)

    assert "exomiser" not in set(delta["task_type"])

    trio = delta.query("task_id == 63")
    assert sorted(trio["seq_id"]) == [1, 2, 3]
    proband = trio.query("family_role == 'proband'")
    assert list(proband["seq_id"]) == [1]
    assert proband.iloc[0]["exomiser_filepath"].endswith("CEPH-1463.exomiser.variants.tsv")
    assert trio.query("family_role != 'proband'")["exomiser_filepath"].isna().all()

    (solo,) = delta.query("task_id == 65").itertuples()
    assert (solo.seq_id, solo.family_role) == (22, "proband")
    assert solo.exomiser_filepath.endswith("SH032.HG00513.exomiser.variants.tsv")


@pytest.fixture
def cnv_exomiser_reports(postgres_clinical_seeds, postgres_instance):
    """Two CNV Exomiser tsvs on the case-1 proband (experiment 1), on ids far above the seeded range:

    - task 9601, a germline SNV annotation task whose tsv has the `exomiser_cnv` data type;
    - task 9602, a `radiant_germline_cnv_annotation` task, with its slivar VCF and its tsv.
    """
    execute_clinical(
        postgres_instance,
        """
        INSERT INTO task (id, task_type_code, created_on, pipeline_name, pipeline_version, genome_build, tenant_code)
        VALUES (9601, 'radiant_germline_annotation', '2026-01-02', 'snv-post-processing', 'mock', 'GRch38', 'radiant'),
               (9602, 'radiant_germline_cnv_annotation', '2026-01-02', 'cnv-post-processing', 'mock', 'GRch38',
                'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id) VALUES (9601, 1, 1), (9602, 1, 1);
        INSERT INTO document (id, name, data_category_code, data_type_code, format_code, size, url, hash, tenant_code)
        VALUES (9601, 'mock-9601.snv.vcf.gz', 'genomic', 'snv', 'vcf', 1, 's3://test-vcf/mock-9601.snv.vcf.gz',
                'mock9601', 'radiant'),
               (9602, 'mock-9602.exomiser.variants.tsv', 'genomic', 'exomiser_cnv', 'tsv', 1,
                's3://test-vcf/mock-9602.exomiser.variants.tsv', 'mock9602', 'radiant'),
               (9603, 'mock-9603.cnv.slivar.vcf.gz', 'genomic', 'gcnv', 'vcf', 1,
                's3://test-vcf/mock-9603.cnv.slivar.vcf.gz', 'mock9603', 'radiant'),
               (9604, 'mock-9604.exomiser.variants.tsv', 'genomic', 'exomiser_cnv', 'tsv', 1,
                's3://test-vcf/mock-9604.exomiser.variants.tsv', 'mock9604', 'radiant');
        INSERT INTO task_has_document (task_id, document_id, type)
        VALUES (9601, 9601, 'output'), (9601, 9602, 'output'), (9602, 9603, 'output'), (9602, 9604, 'output');
        """,
    )
    yield
    execute_clinical(
        postgres_instance,
        """
        DELETE FROM task_has_document WHERE task_id IN (9601, 9602);
        DELETE FROM task_context WHERE task_id IN (9601, 9602);
        DELETE FROM task WHERE id IN (9601, 9602);
        DELETE FROM document WHERE id IN (9601, 9602, 9603, 9604);
        """,
    )


def test_sequencing_experiment_delta_ignores_cnv_exomiser_reports(
    cnv_exomiser_reports, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """The CNV Exomiser report must stay out of the SNV `exomiser` table: the view reads only the `exomiser`
    data type, and never reads a `radiant_germline_cnv_annotation` task."""
    delta = _delta(starrocks_session, sequencing_delta_columns)

    (snv_annotation,) = delta.query("task_id == 9601").itertuples()
    assert snv_annotation.vcf_filepath == "s3://test-vcf/mock-9601.snv.vcf.gz"
    assert pd.isna(snv_annotation.exomiser_filepath)

    assert delta.query("task_id == 9602").empty


@pytest.fixture
def shared_sequencing(postgres_clinical_seeds, postgres_instance):
    """Proband 9610 is sequenced once (experiment 9610) and analysed twice, on ids far above the seeded range:

    - trio case 9610: the proband with father 9611 and mother 9612, annotated by task 9610;
    - solo case 9611: the same proband alone, annotated by task 9611 over the same experiment 9610;
    - alignment task 9612 over experiment 9610, with a NULL case_id: it belongs to the experiment, not to a case.
    """
    execute_clinical(
        postgres_instance,
        """
        INSERT INTO patient (id, submitter_patient_id, organization_code, sex_code, date_of_birth, life_status_code,
                             tenant_code)
        VALUES (9610, 'PT-9610', 'CQGC', 'female', '2015-01-01', 'alive', 'radiant'),
               (9611, 'PT-9611', 'CQGC', 'male',   '1985-01-01', 'alive', 'radiant'),
               (9612, 'PT-9612', 'CQGC', 'female', '1986-01-01', 'alive', 'radiant');
        INSERT INTO cases (id, proband_id, project_id, status_code, primary_condition, created_on, updated_on,
                           case_type_code, submitter_case_id, tenant_code)
        VALUES (9610, 9610, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'germline', 'SHARED-TRIO', 'radiant'),
               (9611, 9610, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'germline', 'SHARED-SOLO', 'radiant');
        INSERT INTO family (id, case_id, family_member_id, relationship_to_proband_code, affected_status_code,
                            tenant_code)
        VALUES (9610, 9610, 9610, 'proband', 'affected',     'radiant'),
               (9611, 9610, 9611, 'father',  'non_affected', 'radiant'),
               (9612, 9610, 9612, 'mother',  'non_affected', 'radiant'),
               (9613, 9611, 9610, 'proband', 'affected',     'radiant');
        INSERT INTO sample (id, type_code, histology_code, submitter_sample_id, patient_id, organization_code,
                            tenant_code)
        VALUES (9610, 'dna', 'normal', 'S9610', 9610, 'CQGC', 'radiant'),
               (9611, 'dna', 'normal', 'S9611', 9611, 'CQGC', 'radiant'),
               (9612, 'dna', 'normal', 'S9612', 9612, 'CQGC', 'radiant');
        INSERT INTO sequencing_experiment (id, sample_id, status_code, aliquot, created_on, updated_on,
                                           experimental_strategy_code, platform_code, tenant_code)
        VALUES (9610, 9610, 'completed', 'A9610', '2026-01-01', '2026-01-01', 'wxs', 'illumina', 'radiant'),
               (9611, 9611, 'completed', 'A9611', '2026-01-01', '2026-01-01', 'wxs', 'illumina', 'radiant'),
               (9612, 9612, 'completed', 'A9612', '2026-01-01', '2026-01-01', 'wxs', 'illumina', 'radiant');
        INSERT INTO case_has_sequencing_experiment (case_id, sequencing_experiment_id)
        VALUES (9610, 9610), (9610, 9611), (9610, 9612), (9611, 9610);
        INSERT INTO task (id, task_type_code, created_on, pipeline_name, pipeline_version, genome_build, tenant_code)
        VALUES (9610, 'radiant_germline_annotation', '2026-01-02', 'snv-post-processing', 'mock', 'GRch38', 'radiant'),
               (9611, 'radiant_germline_annotation', '2026-01-02', 'snv-post-processing', 'mock', 'GRch38', 'radiant'),
               (9612, 'alignment_germline_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id)
        VALUES (9610, 9610, 9610), (9610, 9610, 9611), (9610, 9610, 9612),
               (9611, 9611, 9610),
               (9612, null, 9610);
        INSERT INTO document (id, name, data_category_code, data_type_code, format_code, size, url, hash, tenant_code)
        VALUES (9610, 'trio.snv.vcf.gz', 'genomic', 'snv', 'vcf', 1, 's3://test-vcf/trio.snv.vcf.gz', 'mock9610',
                'radiant'),
               (9611, 'solo.snv.vcf.gz', 'genomic', 'snv', 'vcf', 1, 's3://test-vcf/solo.snv.vcf.gz', 'mock9611',
                'radiant'),
               (9612, 'a9610.cnv.vcf.gz', 'genomic', 'gcnv', 'vcf', 1, 's3://test-vcf/a9610.cnv.vcf.gz', 'mock9612',
                'radiant');
        INSERT INTO task_has_document (task_id, document_id, type)
        VALUES (9610, 9610, 'output'), (9611, 9611, 'output'), (9612, 9612, 'output');
        """,
    )
    yield
    execute_clinical(
        postgres_instance,
        """
        DELETE FROM task_has_document WHERE task_id IN (9610, 9611, 9612);
        DELETE FROM task_context WHERE task_id IN (9610, 9611, 9612);
        DELETE FROM task WHERE id IN (9610, 9611, 9612);
        DELETE FROM document WHERE id IN (9610, 9611, 9612);
        DELETE FROM case_has_sequencing_experiment WHERE case_id IN (9610, 9611);
        DELETE FROM sequencing_experiment WHERE id IN (9610, 9611, 9612);
        DELETE FROM sample WHERE id IN (9610, 9611, 9612);
        DELETE FROM family WHERE case_id IN (9610, 9611);
        DELETE FROM cases WHERE id IN (9610, 9611);
        DELETE FROM patient WHERE id IN (9610, 9611, 9612);
        """,
    )


def test_sequencing_experiment_delta_keeps_a_shared_experiment_in_its_own_cases_tasks(
    shared_sequencing, starrocks_session, sequencing_experiment_tables, sequencing_delta_columns
):
    """RAD-171: an experiment in two cases gets, in each case, the annotation task of that case only, once, with that
    case's family. The alignment task, whose case_id is NULL, stays in both cases."""
    delta = _delta(starrocks_session, sequencing_delta_columns).query("case_id in [9610, 9611]")

    assert not delta.duplicated(["case_id", "seq_id", "task_id"]).any()
    assert sorted(
        (int(r.case_id), int(r.seq_id), int(r.task_id), int(r.family_id), r.family_role) for r in delta.itertuples()
    ) == [
        (9610, 9610, 9610, 9610, "proband"),
        (9610, 9610, 9612, 9610, "proband"),
        (9610, 9611, 9610, 9611, "father"),
        (9610, 9612, 9610, 9612, "mother"),
        (9611, 9610, 9611, 9613, "proband"),
        (9611, 9610, 9612, 9613, "proband"),
    ]
    assert set(delta.query("task_id == 9610")["vcf_filepath"]) == {"s3://test-vcf/trio.snv.vcf.gz"}
    assert set(delta.query("task_id == 9611")["vcf_filepath"]) == {"s3://test-vcf/solo.snv.vcf.gz"}


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
                         AND task_id = 65
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
    # The 16-row baseline less task 65, the only one of the two inserts that the view actually emits:
    # seq 4 / task 4 matches no row in staging_external_sequencing_experiment, so it subtracts nothing.
    assert len(result_df) == 15


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
                         AND task_id = 65
                       """)
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    # The 16-row baseline less task 65, which was just inserted into staging_sequencing_experiment.
    assert len(result_df) == 15

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

    # Should capture the updated experiment -- task 65 is back, so the full 16-row baseline again.
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 16
