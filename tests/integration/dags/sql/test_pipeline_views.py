"""The four RAD-287 views over a real StarRocks + clinical Postgres.

The unit tests (`tests/unit/dags/test_pipeline_views_sql_render.py`) pin what the views say; these check what they
return. Seeds: case 7 is revoked, case 16 a germline trio (proband 44, father 45, mother 46), case 22 a somatic case
whose proband 3 has a tumor (experiment 62) and a normal (experiment 63).

The re-sequenced somatic proband is added on ids 98xx and removed after the test.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from tests.integration.fixtures_common import execute_clinical

pytestmark = pytest.mark.usefixtures("pipeline_views")

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")

REVOKED_CASE_ID = 7
TRIO_CASE_ID = 16
TRIO_MEMBERS = {44: "proband", 45: "father", 46: "mother"}
TUMOR_NORMAL_CASE_ID = 22
TUMOR_NORMAL_PROBAND_ID = 3
SEEDED_TUMOR_EXPERIMENT = 62
SEEDED_NORMAL_EXPERIMENT = 63
NEW_TUMOR = 9801
UNFINISHED = 9802
TRIO_FATHER_ID = 45


def _rows(starrocks_session, sql, parameters=None):
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql, parameters or {})
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _view(starrocks_session, radiant_mapping, view, case_id):
    return _rows(
        starrocks_session,
        f"SELECT * FROM {radiant_mapping[f'starrocks_{view}']} WHERE case_id = %(case_id)s",
        {"case_id": case_id},
    )


@pytest.fixture
def resequenced_tumor(postgres_clinical_seeds, postgres_instance):
    """Case 22's proband gets a second `completed` tumor experiment, newer than the seeded one, aligned by a
    somatic alignment task that published a DRAGEN metrics tarball."""
    execute_clinical(
        postgres_instance,
        f"""
        INSERT INTO sample (id, type_code, parent_sample_id, tissue_site, histology_code, submitter_sample_id,
                            patient_id, organization_code, tenant_code)
        VALUES ({NEW_TUMOR}, 'blood', NULL, NULL, 'tumoral', 'B-9801', {TUMOR_NORMAL_PROBAND_ID}, 'CQGC', 'radiant');
        INSERT INTO sequencing_experiment (id, sample_id, status_code, aliquot, sequencing_lab_code, run_name,
                                           run_alias, run_date, capture_kit, created_on, updated_on,
                                           experimental_strategy_code, sequencing_read_technology_code,
                                           platform_code, tenant_code)
        VALUES ({NEW_TUMOR}, {NEW_TUMOR}, 'completed', 'ALIQUOT-9801-T', 'CQGC', 1677, 'RUN-9801-Tumor',
                '2022-01-01', NULL, '2022-01-01 00:00:00', '2022-01-01 00:00:00', 'wgs', 'short_read',
                'illumina', 'radiant');
        INSERT INTO case_has_sequencing_experiment (case_id, sequencing_experiment_id)
        VALUES ({TUMOR_NORMAL_CASE_ID}, {NEW_TUMOR});
        INSERT INTO task (id, task_type_code, created_on, pipeline_name, pipeline_version, genome_build, tenant_code)
        VALUES ({NEW_TUMOR}, 'alignment_somatic_variant_calling', '2022-01-02 00:00:00', 'dragen', '4.2', 'GRCh38',
                'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id) VALUES ({NEW_TUMOR}, NULL, {NEW_TUMOR});
        INSERT INTO document (id, name, data_category_code, data_type_code, format_code, size, url, hash, tenant_code)
        VALUES ({NEW_TUMOR}, 'tumor-9801.extra.tgz', 'genomic', 'ssup', 'tgz', 1,
                's3://test-vcf/dragen/tumor-9801.extra.tgz', 'hash9801', 'radiant');
        INSERT INTO task_has_document (task_id, document_id, type) VALUES ({NEW_TUMOR}, {NEW_TUMOR}, 'output');
        """,
    )
    yield
    execute_clinical(
        postgres_instance,
        f"""
        DELETE FROM task_has_document WHERE task_id = {NEW_TUMOR};
        DELETE FROM document WHERE id = {NEW_TUMOR};
        DELETE FROM task_context WHERE task_id = {NEW_TUMOR};
        DELETE FROM task WHERE id = {NEW_TUMOR};
        DELETE FROM case_has_sequencing_experiment WHERE sequencing_experiment_id = {NEW_TUMOR};
        DELETE FROM sequencing_experiment WHERE id = {NEW_TUMOR};
        DELETE FROM sample WHERE id = {NEW_TUMOR};
        """,
    )


@pytest.fixture
def unfinished_resequencing(postgres_clinical_seeds, postgres_instance):
    """Case 16's father gets a newer experiment that is only `submitted`."""
    execute_clinical(
        postgres_instance,
        f"""
        INSERT INTO sample (id, type_code, parent_sample_id, tissue_site, histology_code, submitter_sample_id,
                            patient_id, organization_code, tenant_code)
        VALUES ({UNFINISHED}, 'blood', NULL, NULL, 'normal', 'B-9802', {TRIO_FATHER_ID}, 'CQGC', 'radiant');
        INSERT INTO sequencing_experiment (id, sample_id, status_code, aliquot, sequencing_lab_code, run_name,
                                           run_alias, run_date, capture_kit, created_on, updated_on,
                                           experimental_strategy_code, sequencing_read_technology_code,
                                           platform_code, tenant_code)
        VALUES ({UNFINISHED}, {UNFINISHED}, 'submitted', 'ALIQUOT-9802', 'CQGC', 1677, 'RUN-9802',
                '2022-01-01', NULL, '2022-01-01 00:00:00', '2022-01-01 00:00:00', 'wgs', 'short_read',
                'illumina', 'radiant');
        INSERT INTO case_has_sequencing_experiment (case_id, sequencing_experiment_id)
        VALUES ({TRIO_CASE_ID}, {UNFINISHED});
        """,
    )
    yield
    execute_clinical(
        postgres_instance,
        f"""
        DELETE FROM case_has_sequencing_experiment WHERE sequencing_experiment_id = {UNFINISHED};
        DELETE FROM sequencing_experiment WHERE id = {UNFINISHED};
        DELETE FROM sample WHERE id = {UNFINISHED};
        """,
    )


def test_pipeline_case_leaves_out_a_revoked_case(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    assert _view(starrocks_session, radiant_mapping, "pipeline_case", REVOKED_CASE_ID) == []
    (trio,) = _view(starrocks_session, radiant_mapping, "pipeline_case", TRIO_CASE_ID)
    assert trio["case_type"] == "germline"
    assert trio["proband_id"] == 44
    assert trio["project_code"] is not None


def test_the_other_views_follow_pipeline_case(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    assert _view(starrocks_session, radiant_mapping, "current_experiment", REVOKED_CASE_ID) == []
    assert _view(starrocks_session, radiant_mapping, "case_member", REVOKED_CASE_ID) == []


def test_current_experiment_has_one_row_per_member_of_a_germline_case(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    rows = _view(starrocks_session, radiant_mapping, "current_experiment", TRIO_CASE_ID)
    assert sorted(r["patient_id"] for r in rows) == sorted(TRIO_MEMBERS)


def test_an_unfinished_experiment_never_supersedes_a_completed_one(
    unfinished_resequencing, starrocks_session, radiant_mapping
):
    """`completed` is a whitelist: newer sequencing that is not finished is not a candidate."""
    rows = _view(starrocks_session, radiant_mapping, "current_experiment", TRIO_CASE_ID)
    (father,) = [r for r in rows if r["patient_id"] == TRIO_FATHER_ID]
    assert father["seq_id"] != UNFINISHED


def test_current_alignment_has_one_row_per_experiment_and_type(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    duplicates = _rows(
        starrocks_session,
        f"SELECT seq_id, task_type FROM {radiant_mapping['starrocks_current_alignment']} "
        "GROUP BY seq_id, task_type HAVING COUNT(*) > 1",
    )
    assert duplicates == []


def test_case_member_lists_the_trio_with_its_roles(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _view(starrocks_session, radiant_mapping, "case_member", TRIO_CASE_ID)
    assert {r["patient_id"]: r["role"] for r in rows} == TRIO_MEMBERS


def test_a_resequenced_somatic_proband_keeps_its_newest_tumor_and_its_normal(
    resequenced_tumor, starrocks_session, radiant_mapping
):
    rows = _view(starrocks_session, radiant_mapping, "current_experiment", TUMOR_NORMAL_CASE_ID)
    assert {(r["patient_id"], r["histology_code"], r["seq_id"]) for r in rows} == {
        (TUMOR_NORMAL_PROBAND_ID, "tumoral", NEW_TUMOR),
        (TUMOR_NORMAL_PROBAND_ID, "normal", SEEDED_NORMAL_EXPERIMENT),
    }


def test_somatic_qc_runs_on_the_newest_tumor(resequenced_tumor, starrocks_session, radiant_mapping):
    with open(os.path.join(_CLINICAL_SQL, "qc_somatic_facts_select.sql")) as f:
        sql = jinja2.Template(f.read()).render(mapping=radiant_mapping)
    rows = [r for r in _rows(starrocks_session, sql) if r["case_id"] == TUMOR_NORMAL_CASE_ID]
    assert [(r["seq_id"], r["alignment_task_id"], r["annotated"]) for r in rows] == [(NEW_TUMOR, NEW_TUMOR, 0)]
    assert SEEDED_TUMOR_EXPERIMENT not in {r["seq_id"] for r in rows}
