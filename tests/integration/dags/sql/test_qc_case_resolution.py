"""Execute `pending_quality_control_select.sql` against a real StarRocks + clinical Postgres.

The unit tests pin what the template says; what only a database shows is that the joins hold
over the JDBC catalog and that the one-row-per-document shape folds back into members.

Case 16 in the seeds is the happy path: a trio whose three alignments each published a CRAM and
its index, and no QC task. Case 1 carries task 71, a `quality_control_metrics` over all three
of its experiments, and must not come back.
"""

import os

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.qc.resolve import fold_rows, select_cases

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")
DISCOVERY = "pending_quality_control_select.sql"

TRIO_CASE_ID = 16
QC_DONE_CASE_ID = 1
SOMATIC_CASE_ID = 22
TRIO_PROBAND_ALIGNMENT_TASK = 44


def _render(radiant_mapping, params=None):
    with open(os.path.join(_CLINICAL_SQL, DISCOVERY)) as f:
        return jinja2.Template(f.read()).render(
            mapping=radiant_mapping,
            params=params if params is not None else {"task_ids": [], "tenants": []},
        )


def _discover(starrocks_session, radiant_mapping, case_id=None, parameters=None, params=None):
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render(radiant_mapping, params), parameters or {})
        columns = [d[0] for d in cursor.description]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    return [r for r in rows if case_id is None or r["case_id"] == case_id]


def test_a_trio_with_crams_and_no_qc_task_is_discovered(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    members = fold_rows(rows)
    assert sorted(m.role for m in members) == ["father", "mother", "proband"]
    assert {m.patient_id for m in members} == {44, 45, 46}
    assert all(m.exclusion_reason is None for m in members)
    assert all(m.cram_matches == 1 and m.cram_url and m.crai_url for m in members)
    assert all(m.aliquot for m in members)


def test_every_alignment_output_comes_back_as_its_own_row(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """The metrics probe needs the directory of every output, so the proband's alignment
    (task 44: CRAM, index, CNV VCF, index) yields four rows that fold into one member."""
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    proband_rows = [r for r in rows if r["patient_id"] == 44]
    assert len(proband_rows) == 4
    assert {(r["document_data_type"], r["document_format"]) for r in proband_rows} >= {
        ("alignment", "cram"),
        ("alignment", "crai"),
    }
    # The seeds give every document of an alignment the same directory-only url, so the four
    # rows fold into one distinct url; what matters is that folding keeps them all.
    proband = next(m for m in fold_rows(rows) if m.patient_id == 44)
    assert proband.document_urls == sorted({r["document_url"] for r in proband_rows})


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
        params={"task_ids": [TRIO_PROBAND_ALIGNMENT_TASK], "tenants": []},
    )
    assert {r["case_id"] for r in rows} == {TRIO_CASE_ID}
    assert {r["patient_id"] for r in rows} == {44, 45, 46}


def test_an_ungranted_tenant_is_reported_not_hidden(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(
        starrocks_session,
        radiant_mapping,
        TRIO_CASE_ID,
        parameters={"tenants": ["some_other_tenant"]},
        params={"task_ids": [], "tenants": ["some_other_tenant"]},
    )
    assert rows and all(r["exclusion_reason"] == "tenant_not_granted" for r in rows)
