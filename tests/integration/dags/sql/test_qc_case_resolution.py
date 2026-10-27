"""Execute `pending_quality_control_select.sql` against a real StarRocks + clinical Postgres.

The unit tests pin what the template says; what only a database shows is that the joins hold
over the JDBC catalog and that the query returns exactly one row per member.

Case 16 in the seeds is the happy path: a trio whose three alignments each published a CRAM and
its index, and no QC task. Case 1 carries task 71, a `quality_control_metrics` over all three
of its experiments, and must not come back.

The seeds have no DRAGEN metrics tarball. The tarball fixtures add `ssup`/`tgz` outputs to the
proband's alignment (task 44) on document ids 96xx, and remove them after each test.
"""

import os

import jinja2
import psycopg2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.qc.resolve import select_cases

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")
DISCOVERY = "pending_quality_control_select.sql"

TRIO_CASE_ID = 16
TRIO_PROBAND_ID = 44
QC_DONE_CASE_ID = 1
SOMATIC_CASE_ID = 22
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


def test_a_somatic_case_is_never_discovered(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    assert _discover(starrocks_session, radiant_mapping, SOMATIC_CASE_ID) == []


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
