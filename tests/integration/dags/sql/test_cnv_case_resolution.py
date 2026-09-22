"""Execute `pending_cnv_annotation_select.sql` against a real StarRocks + clinical Postgres.

The unit tests pin what the template says; what only a database shows is that the joins hold
over the JDBC catalog and the rows resolve into families.

Case 16 in the seeds is the happy path: a trio whose three alignments each published a
germline CNV VCF (documents 268-273) plus a CRAM and its index, and no CNV annotation. Case 1
carries task 72, a `radiant_germline_cnv_annotation` over all three of its experiments, and
must not come back although its alignments also publish `gcnv` VCFs. Case 8 is the other
seeded case with a `gcnv` VCF and no CNV annotation, and comes back too.
"""

import os

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.cnv.resolve import resolve_families, select_cases

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")
DISCOVERY = "pending_cnv_annotation_select.sql"

TRIO_CASE_ID = 16
CNV_ANNOTATED_CASE_ID = 1
# Case 8: SNV-annotated, but its alignment (task 22) published a `gcnv` VCF never post-processed.
SNV_ANNOTATED_SINGLETON_CASE_ID = 8
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


def test_a_trio_with_cnv_vcfs_and_no_cnv_annotation_is_discovered(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert [r["role"] for r in rows] == ["proband", "father", "mother"]
    assert {r["patient_id"] for r in rows} == {44, 45, 46}
    assert all(r["exclusion_reason"] is None for r in rows)
    assert all(r["gcnv_matches"] == 1 and r["gcnv_url"].endswith(".cnv.vcf.gz") for r in rows)
    assert all(r["cram_url"] and r["crai_url"] for r in rows)
    assert all(r["alignment_pipeline"] == "Dragen" for r in rows)
    assert all(r["aliquot"] for r in rows)


def test_the_rows_resolve_into_one_family(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _discover(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    selection = select_cases(rows)
    assert selection.case_ids == [TRIO_CASE_ID]
    (family,) = resolve_families([m.model_dump() for m in selection.members], [])
    assert family.family_id == "CA16"
    assert [m.role for m in family.members] == ["proband", "father", "mother"]
    # The seeds register every alignment document under one directory-only url, so the
    # index is not at `<cram>.crai` and the family runs without depth refinement.
    assert family.crams_complete is False


def test_a_cnv_annotated_case_is_not_discovered(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    assert _discover(starrocks_session, radiant_mapping, CNV_ANNOTATED_CASE_ID) == []


def test_a_somatic_case_is_never_discovered(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    assert _discover(starrocks_session, radiant_mapping, SOMATIC_CASE_ID) == []


def test_only_cases_with_a_cnv_vcf_are_candidates(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """Three seeded cases carry `gcnv` documents: 1 (already CNV-annotated), 16 (the trio) and 8
    (a singleton whose SNV annotation is done -- irrelevant here, CNV eligibility is its own
    anti-join). Every other case is silence, not a `no_gcnv` exclusion."""
    rows = _discover(starrocks_session, radiant_mapping)
    assert {r["case_id"] for r in rows} == {TRIO_CASE_ID, SNV_ANNOTATED_SINGLETON_CASE_ID}


def test_a_targeted_run_returns_the_whole_case(postgres_clinical_seeds, starrocks_session, radiant_mapping):
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
