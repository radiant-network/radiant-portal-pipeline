"""Execute `pending_quality_control_select.sql` against a real StarRocks + clinical Postgres.

The unit tests pin what the template says; what only a database shows is that the joins hold
over the JDBC catalog and that the query returns exactly one row per member.

Case 16 in the seeds is the happy path: a trio whose three alignments each published a CRAM and
its index, and no QC task. Case 1 carries task 71, a `quality_control_metrics` over all three
of its experiments, and must not come back.

The seeds have no DRAGEN metrics tarball. The tarball fixtures add `ssup`/`tgz` outputs to the
proband's alignment (task 44) on document ids 96xx, and remove them after each test.

Somatic: case 23 holds a tumor alone (experiment 64, alignment task 76, no family row) and case 22 a
tumor and a normal (experiments 62 and 63, no somatic alignment in the seeds). Only the tumor is
QC'd. Task 76 has no output document in the seeds, so the somatic fixtures add its tarball (a CRAM
only where a test shows that it is ignored) and the other shapes on ids 97xx, and remove them after each test.
"""

import os

import jinja2
import psycopg2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.qc.resolve import select_cases
from tests.integration.fixtures_common import execute_clinical

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")
DISCOVERY = "pending_quality_control_select.sql"

TRIO_CASE_ID = 16
TRIO_PROBAND_ID = 44
QC_DONE_CASE_ID = 1
TUMOR_NORMAL_CASE_ID = 22
TUMOR_NORMAL_PROBAND_ID = 3
TUMOR_NORMAL_TUMOR_EXPERIMENT = 62
TUMOR_ONLY_CASE_ID = 23
TUMOR_ONLY_PROBAND_ID = 62
TUMOR_ONLY_EXPERIMENT = 64
TUMOR_ONLY_ALIGNMENT_TASK = 76
TRIO_PROBAND_ALIGNMENT_TASK = 44
TARBALL_URL = "s3://test-vcf/dragen/NA12878.extra.tgz"


def _render(radiant_mapping, params=None):
    with open(os.path.join(_CLINICAL_SQL, DISCOVERY)) as f:
        return jinja2.Template(f.read()).render(
            mapping=radiant_mapping,
            params=params if params is not None else {"task_ids": [], "excluded_tenants": []},
        )


def _discover(starrocks_session, radiant_mapping, case_id=None, parameters=None, params=None):
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render(radiant_mapping, params), parameters or {})
        columns = [d[0] for d in cursor.description]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    return [r for r in rows if case_id is None or r["case_id"] == case_id]


def _execute(postgres_instance, sql):
    with (
        psycopg2.connect(
            host="localhost",
            port=postgres_instance.port,
            user=postgres_instance.user,
            password=postgres_instance.password,
            database=postgres_instance.radiant_db,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(f"SET search_path TO {postgres_instance.radiant_db_schema};")
        cur.execute(sql)
        conn.commit()


def _add_tarballs(postgres_instance, urls_by_id: dict[int, str]):
    documents = ", ".join(
        f"({i}, 'tarball-{i}.tgz', 'genomic', 'ssup', 'tgz', 1, '{url}', 'hash{i}', 'radiant')"
        for i, url in urls_by_id.items()
    )
    links = ", ".join(f"({TRIO_PROBAND_ALIGNMENT_TASK}, {i}, 'output')" for i in urls_by_id)
    _execute(
        postgres_instance,
        f"""
        INSERT INTO document (id, name, data_category_code, data_type_code, format_code, size, url, hash, tenant_code)
        VALUES {documents};
        INSERT INTO task_has_document (task_id, document_id, type) VALUES {links};
        """,
    )


def _remove_documents(postgres_instance, document_ids):
    ids = ", ".join(str(i) for i in document_ids)
    _execute(
        postgres_instance,
        f"""
        DELETE FROM task_has_document WHERE document_id IN ({ids});
        DELETE FROM document WHERE id IN ({ids});
        """,
    )


@pytest.fixture
def proband_tarball(postgres_clinical_seeds, postgres_instance):
    _add_tarballs(postgres_instance, {9601: TARBALL_URL})
    yield
    _remove_documents(postgres_instance, [9601])


@pytest.fixture
def proband_two_tarballs(postgres_clinical_seeds, postgres_instance):
    _add_tarballs(postgres_instance, {9611: TARBALL_URL, 9612: "s3://test-vcf/dragen/NA12878.other.tgz"})
    yield
    _remove_documents(postgres_instance, [9611, 9612])


def test_a_trio_with_crams_and_no_qc_task_is_discovered(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert sorted(r["role"] for r in rows) == ["father", "mother", "proband"]
    assert {r["patient_id"] for r in rows} == {44, 45, 46}
    assert all(r["exclusion_reason"] is None for r in rows)
    assert all(r["cram_matches"] == 1 and r["cram_url"] and r["crai_url"] for r in rows)
    assert all(r["aliquot"] for r in rows)


def test_one_row_per_member_however_many_documents(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """The proband's alignment (task 44) has six outputs: CRAM, index, gVCF, index, germline
    CNV VCF, index. It still comes back once."""
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert [r["patient_id"] for r in rows].count(TRIO_PROBAND_ID) == 1


def test_without_a_tarball_the_member_still_runs(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """The samplesheet falls back to the CRAM's directory."""
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert all(r["dragen_metrics_url"] is None and r["dragen_metrics_matches"] == 0 for r in rows)
    assert select_cases(rows).case_ids == [TRIO_CASE_ID]


def test_the_alignment_s_tarball_comes_back(proband_tarball, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    proband = next(r for r in rows if r["patient_id"] == TRIO_PROBAND_ID)
    assert (proband["dragen_metrics_url"], proband["dragen_metrics_matches"]) == (TARBALL_URL, 1)
    assert proband["exclusion_reason"] is None
    assert len(rows) == 3
    assert select_cases(rows).case_ids == [TRIO_CASE_ID]


def test_two_tarballs_exclude_the_case(proband_two_tarballs, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    proband = next(r for r in rows if r["patient_id"] == TRIO_PROBAND_ID)
    assert proband["dragen_metrics_matches"] == 2
    assert proband["exclusion_reason"] == "ambiguous_dragen_metrics"
    assert len(rows) == 3
    selection = select_cases(rows)
    assert selection.case_ids == []
    assert [e.reason for e in selection.excluded] == ["ambiguous_dragen_metrics"]


def test_the_rows_select_cleanly(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    selection = select_cases(rows)
    assert selection.case_ids == [TRIO_CASE_ID]
    assert [m.role for m in selection.members] == ["proband", "father", "mother"]


def test_a_quality_controlled_case_is_not_discovered(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    assert _discover(starrocks_session, radiant_mapping, QC_DONE_CASE_ID) == []


def test_a_tumor_normal_case_without_a_somatic_alignment_is_not_discovered(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """Its tumor has calling tasks only, and its normal is never a QC member: nothing to run on."""
    assert _discover(starrocks_session, radiant_mapping, TUMOR_NORMAL_CASE_ID) == []


def test_germline_rows_carry_their_case_type(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert {r["case_type"] for r in rows} == {"germline"}


def test_a_targeted_run_returns_the_whole_case(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """`task_ids` narrows candidacy, never membership."""
    rows = _discover(
        starrocks_session,
        radiant_mapping,
        parameters={"task_ids": [TRIO_PROBAND_ALIGNMENT_TASK]},
        params={"task_ids": [TRIO_PROBAND_ALIGNMENT_TASK], "excluded_tenants": []},
    )
    assert {r["case_id"] for r in rows} == {TRIO_CASE_ID}
    assert {r["patient_id"] for r in rows} == {44, 45, 46}


def test_an_excluded_tenant_is_reported_not_hidden(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(
        starrocks_session,
        radiant_mapping,
        TRIO_CASE_ID,
        parameters={"excluded_tenants": ["radiant"]},
        params={"task_ids": [], "excluded_tenants": ["radiant"]},
    )
    assert rows and all(r["exclusion_reason"] == "tenant_excluded" for r in rows)


_TASK_COLUMNS = (
    "INSERT INTO task (id, task_type_code, created_on, pipeline_name, pipeline_version, genome_build, tenant_code)"
)


def _alignment_documents(task_id, first_id, tarball=True, cram=True):
    """A CRAM with its index (unless `cram` is false) and a DRAGEN metrics tarball (unless `tarball`
    is false), as outputs of `task_id`, on document ids `first_id` onwards. Returns the document ids."""
    shapes = []
    if cram:
        shapes += [("alignment", "cram", "cram"), ("alignment", "crai", "cram.crai")]
    if tarball:
        shapes.append(("ssup", "tgz", "extra.tgz"))
    ids = list(range(first_id, first_id + len(shapes)))
    documents = ", ".join(
        f"({i}, 'tumor-{i}.{ext}', 'genomic', '{data_type}', '{file_format}', 1, "
        f"'s3://test-vcf/dragen/tumor-{i}.{ext}', 'hash{i}', 'radiant')"
        for i, (data_type, file_format, ext) in zip(ids, shapes, strict=True)
    )
    links = ", ".join(f"({task_id}, {i}, 'output')" for i in ids)
    return (
        ids,
        f"""
        INSERT INTO document (id, name, data_category_code, data_type_code, format_code, size, url, hash, tenant_code)
        VALUES {documents};
        INSERT INTO task_has_document (task_id, document_id, type) VALUES {links};
    """,
    )


def _cleanup(postgres_instance, document_ids=(), task_ids=(), case_ids=(), family_ids=()):
    sql = ""
    if family_ids:
        sql += f"DELETE FROM family WHERE id IN ({', '.join(str(i) for i in family_ids)});"
    if task_ids:
        tasks = ", ".join(str(i) for i in task_ids)
        sql += f"""
            DELETE FROM task_has_document WHERE task_id IN ({tasks});
            DELETE FROM task_context WHERE task_id IN ({tasks});
            DELETE FROM task WHERE id IN ({tasks});
        """
    if document_ids:
        documents = ", ".join(str(i) for i in document_ids)
        sql += f"""
            DELETE FROM task_has_document WHERE document_id IN ({documents});
            DELETE FROM document WHERE id IN ({documents});
        """
    if case_ids:
        cases = ", ".join(str(i) for i in case_ids)
        sql += f"""
            DELETE FROM case_has_sequencing_experiment WHERE case_id IN ({cases});
            DELETE FROM cases WHERE id IN ({cases});
        """
    execute_clinical(postgres_instance, sql)


@pytest.fixture
def tumor_alignment(postgres_clinical_seeds, postgres_instance):
    """Task 76, the current alignment of case 23's tumor, gets its tarball and no CRAM."""
    ids, sql = _alignment_documents(TUMOR_ONLY_ALIGNMENT_TASK, 9701, cram=False)
    execute_clinical(postgres_instance, sql)
    yield ids
    _cleanup(postgres_instance, document_ids=ids)


@pytest.fixture
def tumor_alignment_with_cram(postgres_clinical_seeds, postgres_instance):
    """The same task with a CRAM, its index and a tarball: the CRAM must change nothing."""
    ids, sql = _alignment_documents(TUMOR_ONLY_ALIGNMENT_TASK, 9703)
    execute_clinical(postgres_instance, sql)
    yield ids
    _cleanup(postgres_instance, document_ids=ids)


@pytest.fixture
def tumor_alignment_without_tarball(postgres_clinical_seeds, postgres_instance):
    """The task with a CRAM and its index and no tarball."""
    ids, sql = _alignment_documents(TUMOR_ONLY_ALIGNMENT_TASK, 9706, tarball=False)
    execute_clinical(postgres_instance, sql)
    yield
    _cleanup(postgres_instance, document_ids=ids)


@pytest.fixture
def tumor_alignment_with_two_tarballs(postgres_clinical_seeds, postgres_instance):
    ids, sql = _alignment_documents(TUMOR_ONLY_ALIGNMENT_TASK, 9707, cram=False)
    more, more_sql = _alignment_documents(TUMOR_ONLY_ALIGNMENT_TASK, 9708, cram=False)
    execute_clinical(postgres_instance, sql + more_sql)
    yield
    _cleanup(postgres_instance, document_ids=ids + more)


@pytest.fixture
def tumor_realigned(tumor_alignment, postgres_instance):
    """A newer alignment 9710 of the same tumor experiment, with its own documents: task 76 is superseded."""
    ids, documents = _alignment_documents(9710, 9711, cram=False)
    execute_clinical(
        postgres_instance,
        f"""
        {_TASK_COLUMNS}
        VALUES (9710, 'alignment_somatic_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id)
        VALUES (9710, NULL, {TUMOR_ONLY_EXPERIMENT});
        {documents}
        """,
    )
    yield ids
    _cleanup(postgres_instance, document_ids=ids, task_ids=[9710])


@pytest.fixture
def tumor_normal_alignment(postgres_clinical_seeds, postgres_instance):
    """Case 22's tumor gets a complete somatic alignment 9720, next to the case's normal experiment."""
    ids, documents = _alignment_documents(9720, 9721, cram=False)
    execute_clinical(
        postgres_instance,
        f"""
        {_TASK_COLUMNS}
        VALUES (9720, 'alignment_somatic_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id)
        VALUES (9720, NULL, {TUMOR_NORMAL_TUMOR_EXPERIMENT});
        {documents}
        """,
    )
    yield
    _cleanup(postgres_instance, document_ids=ids, task_ids=[9720])


@pytest.fixture
def revoked_tumor_only_case(tumor_alignment, postgres_instance):
    """Somatic case 9730 holds the same ready tumor experiment as case 23, but is revoked."""
    execute_clinical(
        postgres_instance,
        f"""
        INSERT INTO cases (id, proband_id, project_id, status_code, primary_condition,
                           created_on, updated_on, case_type_code, submitter_case_id, tenant_code)
        VALUES (9730, {TUMOR_ONLY_PROBAND_ID}, 1, 'revoked', 'MONDO:0700092', now(), now(), 'somatic',
                'REVOKED-TUMOR-ONLY', 'radiant');
        INSERT INTO case_has_sequencing_experiment (case_id, sequencing_experiment_id)
        VALUES (9730, {TUMOR_ONLY_EXPERIMENT});
        """,
    )
    yield
    _cleanup(postgres_instance, case_ids=[9730])


@pytest.fixture
def tumor_quality_controlled(tumor_alignment, postgres_instance):
    execute_clinical(
        postgres_instance,
        f"""
        {_TASK_COLUMNS}
        VALUES (9740, 'quality_control_metrics', '2026-01-03', 'quality-control-pipeline', '77d819e', 'GRch38',
                'radiant');
        INSERT INTO task_context (task_id, case_id, sequencing_experiment_id)
        VALUES (9740, {TUMOR_ONLY_CASE_ID}, {TUMOR_ONLY_EXPERIMENT});
        """,
    )
    yield
    _cleanup(postgres_instance, task_ids=[9740])


@pytest.fixture
def tumor_only_family_row(tumor_alignment, postgres_instance):
    execute_clinical(
        postgres_instance,
        f"""
        INSERT INTO family (id, case_id, family_member_id, relationship_to_proband_code, affected_status_code,
                            tenant_code)
        VALUES (9750, {TUMOR_ONLY_CASE_ID}, {TUMOR_ONLY_PROBAND_ID}, 'proband', 'affected', 'radiant');
        """,
    )
    yield
    _cleanup(postgres_instance, family_ids=[9750])


def test_a_tumor_only_case_runs_its_tumor_on_its_current_alignment(
    tumor_alignment, starrocks_session, radiant_mapping
):
    [tarball] = tumor_alignment
    [row] = _discover(starrocks_session, radiant_mapping, TUMOR_ONLY_CASE_ID)
    assert row["case_type"] == "somatic"
    assert (row["role"], row["affected_status"]) == ("proband", "unknown")
    assert (row["patient_id"], row["seq_id"]) == (TUMOR_ONLY_PROBAND_ID, TUMOR_ONLY_EXPERIMENT)
    assert row["aliquot"] == "TCRBOA6_SRX1166091-T"
    assert row["alignment_task_id"] == TUMOR_ONLY_ALIGNMENT_TASK
    assert (row["cram_url"], row["cram_matches"], row["crai_url"]) == (None, 0, None)
    assert row["dragen_metrics_url"].endswith(f"tumor-{tarball}.extra.tgz") and row["dragen_metrics_matches"] == 1
    assert row["exclusion_reason"] is None
    assert select_cases([row]).case_ids == [TUMOR_ONLY_CASE_ID]


def test_a_tumor_s_cram_is_ignored(tumor_alignment_with_cram, starrocks_session, radiant_mapping):
    """The tarball selects the tumor and the CRAM comes back as nothing, as if it were not there."""
    _, _, tarball = tumor_alignment_with_cram
    [row] = _discover(starrocks_session, radiant_mapping, TUMOR_ONLY_CASE_ID)
    assert (row["cram_url"], row["cram_matches"], row["crai_url"]) == (None, 0, None)
    assert row["dragen_metrics_url"].endswith(f"tumor-{tarball}.extra.tgz")
    assert row["exclusion_reason"] is None
    assert select_cases([row]).case_ids == [TUMOR_ONLY_CASE_ID]


def test_a_tumor_only_case_is_selected_next_to_the_germline_ones(tumor_alignment, starrocks_session, radiant_mapping):
    case_ids = select_cases(_discover(starrocks_session, radiant_mapping)).case_ids
    assert {TRIO_CASE_ID, TUMOR_ONLY_CASE_ID} <= set(case_ids)
    # Its tumor has no somatic alignment in the seeds.
    assert TUMOR_NORMAL_CASE_ID not in case_ids


def test_a_targeted_run_takes_a_somatic_alignment(tumor_alignment, starrocks_session, radiant_mapping):
    rows = _discover(
        starrocks_session,
        radiant_mapping,
        parameters={"task_ids": [TUMOR_ONLY_ALIGNMENT_TASK]},
        params={"task_ids": [TUMOR_ONLY_ALIGNMENT_TASK], "excluded_tenants": []},
    )
    assert [r["case_id"] for r in rows] == [TUMOR_ONLY_CASE_ID]
    assert select_cases(rows, [TUMOR_ONLY_ALIGNMENT_TASK]).case_ids == [TUMOR_ONLY_CASE_ID]


def test_a_tumor_with_a_cram_and_no_tarball_comes_back_as_no_metrics(
    tumor_alignment_without_tarball, starrocks_session, radiant_mapping
):
    [row] = _discover(starrocks_session, radiant_mapping, TUMOR_ONLY_CASE_ID)
    assert (row["dragen_metrics_url"], row["dragen_metrics_matches"]) == (None, 0)
    assert row["cram_url"] is None
    assert row["exclusion_reason"] == "no_metrics"
    selection = select_cases([row])
    assert selection.case_ids == []
    assert [e.reason for e in selection.excluded] == ["no_metrics"]


def test_a_tumor_with_two_tarballs_comes_back_as_ambiguous(
    tumor_alignment_with_two_tarballs, starrocks_session, radiant_mapping
):
    [row] = _discover(starrocks_session, radiant_mapping, TUMOR_ONLY_CASE_ID)
    assert row["dragen_metrics_matches"] == 2
    assert row["exclusion_reason"] == "ambiguous_dragen_metrics"
    assert [e.reason for e in select_cases([row]).excluded] == ["ambiguous_dragen_metrics"]


def test_a_superseded_somatic_alignment_is_not_selected(tumor_realigned, starrocks_session, radiant_mapping):
    tarball = tumor_realigned[0]
    [row] = _discover(starrocks_session, radiant_mapping, TUMOR_ONLY_CASE_ID)
    assert row["alignment_task_id"] == 9710
    assert row["dragen_metrics_url"].endswith(f"tumor-{tarball}.extra.tgz")
    stale = _discover(
        starrocks_session,
        radiant_mapping,
        parameters={"task_ids": [TUMOR_ONLY_ALIGNMENT_TASK]},
        params={"task_ids": [TUMOR_ONLY_ALIGNMENT_TASK], "excluded_tenants": []},
    )
    assert stale == []


def test_a_tumor_normal_case_runs_its_tumor_alone(tumor_normal_alignment, starrocks_session, radiant_mapping):
    """The normal experiment of the case is never QC'd here: one member, the tumor."""
    [row] = _discover(starrocks_session, radiant_mapping, TUMOR_NORMAL_CASE_ID)
    assert row["case_type"] == "somatic"
    assert (row["role"], row["patient_id"]) == ("proband", TUMOR_NORMAL_PROBAND_ID)
    assert row["seq_id"] == TUMOR_NORMAL_TUMOR_EXPERIMENT
    assert row["aliquot"] == "TCR002361_SRX1091647-T"
    assert row["alignment_task_id"] == 9720
    assert row["exclusion_reason"] is None
    assert select_cases([row]).case_ids == [TUMOR_NORMAL_CASE_ID]
    targeted = _discover(
        starrocks_session,
        radiant_mapping,
        parameters={"task_ids": [9720]},
        params={"task_ids": [9720], "excluded_tenants": []},
    )
    assert [(r["case_id"], r["seq_id"]) for r in targeted] == [(TUMOR_NORMAL_CASE_ID, TUMOR_NORMAL_TUMOR_EXPERIMENT)]


def test_a_revoked_somatic_case_is_not_discovered(revoked_tumor_only_case, starrocks_session, radiant_mapping):
    case_ids = {r["case_id"] for r in _discover(starrocks_session, radiant_mapping)}
    assert TUMOR_ONLY_CASE_ID in case_ids
    assert 9730 not in case_ids


def test_a_quality_controlled_tumor_is_not_discovered(tumor_quality_controlled, starrocks_session, radiant_mapping):
    assert _discover(starrocks_session, radiant_mapping, TUMOR_ONLY_CASE_ID) == []


def test_a_somatic_family_row_gives_the_affected_status(tumor_only_family_row, starrocks_session, radiant_mapping):
    [row] = _discover(starrocks_session, radiant_mapping, TUMOR_ONLY_CASE_ID)
    assert (row["role"], row["affected_status"]) == ("proband", "affected")
