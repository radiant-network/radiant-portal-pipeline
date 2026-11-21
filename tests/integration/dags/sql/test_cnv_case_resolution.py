"""Execute the germline CNV facts and members queries against a real StarRocks + clinical Postgres.

The unit tests pin what the templates say; what only a database shows is that the joins hold
over the JDBC catalog and the rows decide and resolve into families.

Case 16 in the seeds is the happy path: a trio whose three alignments each published a
germline CNV VCF (documents 268-273) plus a CRAM and its index, and no CNV annotation. Case 1
carries task 72, a `radiant_germline_cnv_annotation` over all three of its experiments, and
must not come back although its alignments also publish `gcnv` VCFs. Case 8 is the other
seeded case with a `gcnv` VCF and no CNV annotation, and comes back too.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.decision import Decision
from radiant.tasks.nextflow.cnv.resolve import decide_cases, resolve_families, select_run_cases

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")

# The queries read the RAD-287 views: create them once for the session.
pytestmark = pytest.mark.usefixtures("pipeline_views")
FACTS = "germline_cnv_facts_select.sql"
MEMBERS = "germline_cnv_case_members_select.sql"

TRIO_CASE_ID = 16
CNV_ANNOTATED_CASE_ID = 1
# Case 8: SNV-annotated, but its alignment (task 22) published a `gcnv` VCF never post-processed.
SNV_ANNOTATED_SINGLETON_CASE_ID = 8
SOMATIC_CASE_ID = 22


def _run(starrocks_session, radiant_mapping, filename, parameters=None):
    with open(os.path.join(_CLINICAL_SQL, filename)) as f:
        sql = jinja2.Template(f.read()).render(mapping=radiant_mapping, params={})
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql, parameters or {})
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _facts(starrocks_session, radiant_mapping, case_id=None):
    rows = _run(starrocks_session, radiant_mapping, FACTS)
    return [r for r in rows if case_id is None or r["case_id"] == case_id]


def test_a_trio_with_cnv_vcfs_and_no_cnv_annotation_runs(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _facts(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert {r["patient_id"] for r in rows} == {44, 45, 46}
    assert all(r["gcnv_matches"] == 1 and r["alignment_pipeline"] == "Dragen" and not r["annotated"] for r in rows)
    (entry,) = decide_cases(rows, [])
    assert (entry.case_id, entry.decision, entry.reasons) == (TRIO_CASE_ID, Decision.RUN, ())


def test_an_excluded_tenant_is_a_skip_not_a_filter(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    (entry,) = decide_cases(_facts(starrocks_session, radiant_mapping, TRIO_CASE_ID), ["radiant"])
    assert entry.decision == Decision.SKIP


def test_only_cases_with_a_cnv_vcf_not_annotated_are_candidates(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """Three seeded cases carry `gcnv` documents: 1 (already CNV-annotated), 16 (the trio) and 8
    (a singleton whose SNV annotation is done -- irrelevant here, CNV has its own anti-join).
    Every other case is silence, not a `missing_input` reason. Somatic cases never come back."""
    case_ids = {r["case_id"] for r in _facts(starrocks_session, radiant_mapping)}
    assert case_ids == {TRIO_CASE_ID, SNV_ANNOTATED_SINGLETON_CASE_ID}
    assert not {CNV_ANNOTATED_CASE_ID, SOMATIC_CASE_ID} & case_ids


def test_the_named_case_resolves_into_one_family(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    rows = _run(starrocks_session, radiant_mapping, MEMBERS, parameters={"case_ids": [TRIO_CASE_ID]})
    assert all(r["gcnv_url"].endswith(".cnv.vcf.gz") and r["cram_url"] and r["crai_url"] for r in rows)
    selection = select_run_cases(rows, [])
    assert selection.case_ids == [TRIO_CASE_ID]
    (family,) = resolve_families([m.model_dump() for m in selection.members], [])
    assert family.family_id == "CA16"
    assert [m.role for m in family.members] == ["proband", "father", "mother"]
    # The seeds register every alignment document under one directory-only url, so the
    # index is not at `<cram>.crai` and the family runs without depth refinement.
    assert family.crams_complete is False
