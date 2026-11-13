"""Execute `somatic_snv_facts_select.sql` against a real StarRocks + clinical Postgres.

The seeds follow the RAD-23 contract. Calling tasks 73 (tumor-normal, case 22), 74 (tumor-only,
case 22) and 70 (tumor-only, case 23) published raw `ssnv` VCFs that are the inputs of the
annotation tasks 67, 68 and 69: they are `done`. Calling tasks 75 (tumor-normal, case 22) and 76
(`alignment_somatic_variant_calling`, case 23, with an `scnv` VCF too) run.

The shapes the seeds do not have are built on ids far above the seeded range, and removed after
each test, so no other integration test can see them:

- `no_ssnv_task`: a calling task with no `ssnv` VCF;
- `shared_task_fixtures`: one calling task framed by two somatic cases, annotated on one only;
- `maf_document`: a MAF on the pending calling task 75, next to its `ssnv` VCF.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.selection import CaseResolutionError
from radiant.tasks.nextflow.somatic_snv.resolve import select_tasks
from tests.integration.fixtures_common import execute_clinical

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")

# The queries read the RAD-287 views: create them once for the session.
pytestmark = pytest.mark.usefixtures("pipeline_views")
DISCOVERY = "somatic_snv_facts_select.sql"

PENDING_TN_TASK = 75
PENDING_TO_TASK = 76
ANNOTATED_TASKS = (70, 73, 74)
NO_SSNV_TASK = 9501
MAF_DOCUMENT = 9531


def _render(radiant_mapping):
    with open(os.path.join(_CLINICAL_SQL, DISCOVERY)) as f:
        return jinja2.Template(f.read()).render(mapping=radiant_mapping)


def _discover(starrocks_session, radiant_mapping, task_id=None):
    """The fact rows, of one calling task when `task_id` is given."""
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render(radiant_mapping))
        columns = [d[0] for d in cursor.description]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    return [r for r in rows if task_id is None or r["calling_task_id"] == task_id]


def _cleanup(postgres_instance, task_ids, document_ids=(), case_ids=()):
    tasks = ", ".join(str(i) for i in task_ids)
    sql = f"""
        DELETE FROM task_has_document WHERE task_id IN ({tasks});
        DELETE FROM task_context WHERE task_id IN ({tasks});
        DELETE FROM task WHERE id IN ({tasks});
    """
    if document_ids:
        sql += f"DELETE FROM document WHERE id IN ({', '.join(str(i) for i in document_ids)});"
    if case_ids:
        cases = ", ".join(str(i) for i in case_ids)
        sql += f"""
            DELETE FROM case_has_sequencing_experiment WHERE case_id IN ({cases});
            DELETE FROM cases WHERE id IN ({cases});
        """
    execute_clinical(postgres_instance, sql)


def _ssnv_document(document_id):
    return (
        f"({document_id}, 'mock-{document_id}.snv.vcf.gz', 'genomic', 'ssnv', 'vcf', 1, "
        f"'s3://test-vcf/mock-{document_id}.snv.vcf.gz', 'mock{document_id}', 'radiant')"
    )


_TASK_COLUMNS = (
    "INSERT INTO task (id, task_type_code, created_on, pipeline_name, pipeline_version, genome_build, tenant_code)"
)
_DOCUMENT_COLUMNS = (
    "INSERT INTO document (id, name, data_category_code, data_type_code, format_code, size, url, hash, tenant_code)"
)


@pytest.fixture
def no_ssnv_task(postgres_clinical_seeds, postgres_instance):
    execute_clinical(
        postgres_instance,
        f"""
        {_TASK_COLUMNS}
        VALUES (9501, 'somatic_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id)
        VALUES (9501, NULL, 62), (9501, NULL, 63);
        """,
    )
    yield
    _cleanup(postgres_instance, [9501])


@pytest.fixture
def shared_task_fixtures(postgres_clinical_seeds, postgres_instance):
    """Calling task 9521 on experiment 64, which case 23 and the extra somatic case 9520 both hold.
    Its VCF is the input of annotation 9522, registered on case 23 only."""
    execute_clinical(
        postgres_instance,
        f"""
        INSERT INTO cases (id, proband_id, project_id, status_code, primary_condition,
                           created_on, updated_on, case_type_code, submitter_case_id, tenant_code)
        VALUES (9520, 62, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'somatic', 'SHARED-SOMATIC', 'radiant');
        INSERT INTO case_has_sequencing_experiment (case_id, sequencing_experiment_id) VALUES (9520, 64);
        {_TASK_COLUMNS}
        VALUES (9521, 'alignment_somatic_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant'),
               (9522, 'radiant_somatic_annotation', '2026-01-02',
                'snv-post-processing', 'b535f27', 'GRch38', 'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id)
        VALUES (9521, NULL, 64), (9522, 23, 64);
        {_DOCUMENT_COLUMNS}
        VALUES {_ssnv_document(9521)};
        INSERT INTO task_has_document (task_id, document_id, type)
        VALUES (9521, 9521, 'output'), (9522, 9521, 'input');
        """,
    )
    yield
    _cleanup(postgres_instance, [9521, 9522], [9521], [9520])


@pytest.fixture
def maf_document(postgres_clinical_seeds, postgres_instance):
    """RAD-140: a somatic MAF, typed `ssnv` like the VCF, published by the seeded calling task 75.
    Task 75 is a seed, so only the document and its link are removed afterwards."""
    execute_clinical(
        postgres_instance,
        f"""
        {_DOCUMENT_COLUMNS}
        VALUES ({MAF_DOCUMENT}, 'mock-{MAF_DOCUMENT}.somatic.maf', 'genomic', 'ssnv', 'maf', 1,
                's3://test-vcf/mock-{MAF_DOCUMENT}.somatic.maf', 'mock{MAF_DOCUMENT}', 'radiant');
        INSERT INTO task_has_document (task_id, document_id, type)
        VALUES ({PENDING_TN_TASK}, {MAF_DOCUMENT}, 'output');
        """,
    )
    yield
    execute_clinical(
        postgres_instance,
        f"""
        DELETE FROM task_has_document WHERE document_id = {MAF_DOCUMENT};
        DELETE FROM document WHERE id = {MAF_DOCUMENT};
        """,
    )


def test_a_tumor_normal_and_a_tumor_only_calling_task_run(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """AC1. And AC7: the calling tasks whose VCF is already an annotation's input are done."""
    rows = _discover(starrocks_session, radiant_mapping)
    by_task = {}
    for row in rows:
        by_task.setdefault(row["calling_task_id"], []).append(row)

    selection = select_tasks(rows)
    assert {t.task_id for t in selection.tasks} == {PENDING_TN_TASK, PENDING_TO_TASK}
    assert {task for task, _ in selection.done} == set(ANNOTATED_TASKS)

    tn = by_task[PENDING_TN_TASK]
    assert [(r["case_id"], r["seq_id"], r["histology"]) for r in tn] == [(22, 62, "tumoral"), (22, 63, "normal")]
    assert not any(r["annotated"] for r in tn)

    [to] = by_task[PENDING_TO_TASK]
    assert (to["case_id"], to["seq_id"], to["calling_task_type"]) == (23, 64, "alignment_somatic_variant_calling")
    assert not to["annotated"]


def test_only_the_ssnv_vcf_is_selected(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """AC3: task 76 publishes an `scnv` VCF too, which is never the trigger."""
    rows = _discover(starrocks_session, radiant_mapping, PENDING_TO_TASK)
    assert [r["ssnv_url"].rsplit("/", 1)[-1] for r in rows] == ["TCRBOA6_SRX1166091-T.v2.snv.vcf.gz"]
    # Counted, the `scnv` VCF would make the task `ambiguous_ssnv`.
    assert rows[0]["ssnv_matches"] == 1


def test_a_maf_next_to_the_ssnv_vcf_is_never_selected(maf_document, starrocks_session, radiant_mapping):
    """RAD-140: the MAF shares the VCF's `ssnv` data type, but only the `vcf` format is the trigger.
    Counted, it would make task 75 `ambiguous_ssnv`: the nightly run would drop it without a word, and a
    targeted run would fail on it. Both runs are checked."""
    vcf_url = "s3://test-vcf/TCR002361_SRX1091647-T_vs_SRX1091646-N.v2.snv.vcf.gz"

    rows = _discover(starrocks_session, radiant_mapping)
    task_rows = [r for r in rows if r["calling_task_id"] == PENDING_TN_TASK]
    assert task_rows
    assert {(r["ssnv_url"], r["ssnv_matches"]) for r in task_rows} == {(vcf_url, 1)}
    assert PENDING_TN_TASK in {t.task_id for t in select_tasks(rows).tasks}
    [task] = select_tasks(rows, [PENDING_TN_TASK]).tasks
    assert task.task_id == PENDING_TN_TASK


def test_the_rows_resolve_into_calling_tasks(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """AC6: the annotation carries the calling task's aliquots, one for an alignment call."""
    selection = select_tasks(_discover(starrocks_session, radiant_mapping))
    by_id = {t.task_id: t for t in selection.tasks}
    assert by_id[PENDING_TN_TASK].aliquots == ["TCR002361_SRX1091647-T", "TCR002361_SRX1091646-N"]
    assert by_id[PENDING_TO_TASK].aliquots == ["TCRBOA6_SRX1166091-T"]
    assert selection.case_ids == [22, 23]
    assert all(t.sequencing_type == "WGS" for t in selection.tasks)


def test_an_annotated_task_named_by_hand_is_an_error(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping)
    assert all(r["annotated"] for r in rows if r["calling_task_id"] == 73)
    with pytest.raises(CaseResolutionError, match=r"\[73\]"):
        select_tasks(rows, [73])


def test_a_task_without_ssnv_has_no_trigger_document_unless_named(no_ssnv_task, starrocks_session, radiant_mapping):
    """AC8: no `ssnv` VCF, no annotation; named by hand, an error naming the task."""
    rows = _discover(starrocks_session, radiant_mapping)
    assert {r["ssnv_matches"] for r in rows if r["calling_task_id"] == NO_SSNV_TASK} == {0}
    assert NO_SSNV_TASK in {task for task, _ in select_tasks(rows).untriggered}
    with pytest.raises(CaseResolutionError, match=f"calling task {NO_SSNV_TASK}"):
        select_tasks(rows, [NO_SSNV_TASK])


def test_an_excluded_tenant_is_reported_not_filtered(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    selection = select_tasks(_discover(starrocks_session, radiant_mapping), excluded_tenants=["radiant"])
    assert selection.tasks == []
    assert {e.reason for e in selection.excluded} == {"tenant_excluded"}


def test_an_annotation_on_one_case_does_not_hide_another_case_of_the_task(
    shared_task_fixtures, starrocks_session, radiant_mapping
):
    rows = _discover(starrocks_session, radiant_mapping, 9521)
    assert sorted(r["case_id"] for r in rows if not r["annotated"]) == [9520]
    [task] = select_tasks(rows, [9521]).tasks
    assert [c.case_id for c in task.cases] == [9520]
