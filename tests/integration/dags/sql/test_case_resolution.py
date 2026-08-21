"""Execute the two case-resolution templates against a real StarRocks + clinical Postgres.

The unit tests cover the rendering and the assertions; what only a real database can show
is that the joins hold, that the cross-catalog join to the shared HPO dictionary works, and
that the doubled `%%` in the LIKE survives `cursor.execute(sql, parameters)`.

Case 16 in the seeds is a proper trio: proband 44 with father 45 and mother 46, and HPO
terms recorded against the proband.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.resolve import REL_ORDER

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")
_OPEN_DATA_INIT = os.path.join(DAGS_DIR, "sql", "open_data", "init")

TRIO_CASE_ID = 16
SOMATIC_CASE_ID = 22


def _render(sql_file, radiant_mapping):
    with open(sql_file) as f:
        return jinja2.Template(f.read()).render(mapping=radiant_mapping)


def _run(starrocks_session, radiant_mapping, filename, parameters):
    sql = _render(os.path.join(_CLINICAL_SQL, filename), radiant_mapping)
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql, parameters)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


@pytest.fixture(scope="session")
def hpo_term_table(starrocks_session, radiant_mapping):
    """The phenotype query joins the shared dictionary, so it has to exist to run at all."""
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render(os.path.join(_OPEN_DATA_INIT, "hpo_term_create_table.sql"), radiant_mapping))
    yield


def test_members_returns_one_row_per_family_member_proband_first(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_members_select.sql",
        {"case_ids": [TRIO_CASE_ID]},
    )
    assert [r["role"] for r in rows] == ["proband", "father", "mother"]
    assert {r["patient_id"] for r in rows} == {44, 45, 46}
    assert all(r["case_id"] == TRIO_CASE_ID for r in rows)
    assert all(r["tenant_code"] == "radiant" for r in rows)


def test_the_proband_ordering_matches_what_the_writers_assume(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """The query orders proband-first and `resolve_families` sorts the same way; if the two
    ever diverged the PED would name the wrong parents."""
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_members_select.sql",
        {"case_ids": [TRIO_CASE_ID]},
    )
    assert [r["role"] for r in rows] == sorted((r["role"] for r in rows), key=lambda role: REL_ORDER[role])


def test_the_case_carries_a_project_code(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """`(project_code, submitter_case_id)` is what the batch PATCH looks a case up by, so a
    null here would fail registration long after the pipeline has run."""
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_members_select.sql",
        {"case_ids": [TRIO_CASE_ID]},
    )
    assert all(r["project_code"] for r in rows)
    assert all(r["submitter_case_id"] for r in rows)


def test_each_member_resolves_to_exactly_one_gvcf(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """The count is the safeguard the whole samplesheet rests on: `resolve_families` accepts
    a member only at exactly 1. Anything else here means the type filter is picking up
    something that is not the member's data file."""
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_members_select.sql",
        {"case_ids": [TRIO_CASE_ID]},
    )
    assert [r["gvcf_matches"] for r in rows] == [1, 1, 1], [(r["sample_id"], r["gvcf_matches"]) for r in rows]


def test_a_case_joint_called_upstream_comes_back_with_no_gvcfs(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """Case 1's members have no registered gVCF. The query returns them rather than
    dropping them, so `resolve_families` can say *why* the case is out of scope instead of
    silently producing a short samplesheet."""
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_members_select.sql",
        {"case_ids": [1]},
    )
    assert rows
    assert all(r["gvcf_matches"] == 0 and r["gvcf_url"] is None for r in rows)


def test_a_somatic_case_returns_nothing(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """The pipeline's `step: genotype` entry point assumes germline joint calling."""
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_members_select.sql",
        {"case_ids": [SOMATIC_CASE_ID]},
    )
    assert rows == []


def test_the_tenant_is_returned_rather_than_required(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """`cases.id` is a single-column primary key over one shared schema, so a case id
    already names its tenant. The DAG reads it off the rows and addresses the batch PATCH
    there, which is why `case_ids` is the only thing a run has to supply."""
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_members_select.sql",
        {"case_ids": [TRIO_CASE_ID]},
    )
    assert {r["tenant_code"] for r in rows} == {"radiant"}


def test_phenotypes_come_back_for_the_proband(
    postgres_clinical_seeds, starrocks_session, radiant_mapping, hpo_term_table
):
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_phenotypes_select.sql",
        {"case_ids": [TRIO_CASE_ID]},
    )
    proband_terms = [r for r in rows if r["patient_id"] == 44]
    assert proband_terms
    assert all(r["hpo_id"].startswith("HP:") for r in proband_terms)
    # Both interpretations are present in the seeds; `negative` becomes `excluded: true`.
    assert {r["interpretation_code"] for r in proband_terms} == {"positive", "negative"}


def test_an_hpo_code_the_dictionary_does_not_know_keeps_its_row(
    postgres_clinical_seeds, starrocks_session, radiant_mapping, hpo_term_table
):
    """A LEFT JOIN on purpose: a missing label is cosmetic, a dropped phenotype is not."""
    rows = _run(
        starrocks_session,
        radiant_mapping,
        "case_phenotypes_select.sql",
        {"case_ids": [TRIO_CASE_ID]},
    )
    # The dictionary is empty in this fixture, so every label is null and every row survives.
    assert rows
    assert all(r["hpo_label"] is None for r in rows)
