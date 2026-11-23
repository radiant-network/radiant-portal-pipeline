"""Execute the discovery and phenotype templates against a real StarRocks + clinical Postgres.

The unit tests cover rendering and the Python-side assertions; what only a real database can
show is that the joins hold, that the window functions behave over the JDBC catalog, and
that parameter binding survives `cursor.execute(sql, parameters)`.

Case 16 in the seeds is the happy path: a proper trio, proband 44 with father 45 and mother
46, each with a `completed` sequencing experiment, an alignment task and a gVCF, and no
annotation. The remaining shapes of SJRA-1698 6 do not exist in the seeds and are built by
`supersession_fixtures` below, on ids far above the seeded range so nothing else shifts.
"""

import os

import jinja2
import psycopg2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.decision import Decision, Reason, ReasonCode
from radiant.tasks.nextflow.resolve import REL_ORDER, decide_cases, select_run_cases

_CLINICAL_SQL = os.path.join(DAGS_DIR, "sql", "clinical")

# The queries read the RAD-287 views: create them once for the session.
pytestmark = pytest.mark.usefixtures("pipeline_views")
_OPEN_DATA_INIT = os.path.join(DAGS_DIR, "sql", "open_data", "init")

FACTS = "germline_snv_facts_select.sql"
MEMBERS = "germline_snv_case_members_select.sql"
PHENOTYPES = "case_phenotypes_select.sql"

TRIO_CASE_ID = 16
SOMATIC_CASE_ID = 22
ANNOTATED_CASE_ID = 8

# Ids for the rows built below. Far above anything in seeds.sql, so an added scenario can
# never collide with a seeded one or renumber it.
SHARED_CASE_A = 9001
SHARED_CASE_B = 9002
SUPERSEDED_CASE = 9003
REALIGNED_CASE = 9004
PENDING_CASE = 9005


def _render(sql_file, radiant_mapping, params=None):
    with open(sql_file) as f:
        return jinja2.Template(f.read()).render(mapping=radiant_mapping, params=params or {})


def _run(starrocks_session, radiant_mapping, filename, parameters=None, params=None):
    sql = _render(os.path.join(_CLINICAL_SQL, filename), radiant_mapping, params)
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql, parameters or {})
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _facts(starrocks_session, radiant_mapping, case_id=None):
    rows = _run(starrocks_session, radiant_mapping, FACTS)
    return [r for r in rows if case_id is None or r["case_id"] == case_id]


def _members(starrocks_session, radiant_mapping, *case_ids):
    return _run(starrocks_session, radiant_mapping, MEMBERS, parameters={"case_ids": list(case_ids)})


def _decided(rows, excluded_tenants=()):
    """{case_id: (decision, reasons)}, as the case scheduler decides on the facts."""
    return {e.case_id: (e.decision, e.reasons) for e in decide_cases(rows, list(excluded_tenants))}


@pytest.fixture(scope="session")
def hpo_term_table(starrocks_session, radiant_mapping):
    """The phenotype query joins the shared dictionary, so it has to exist to run at all."""
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render(os.path.join(_OPEN_DATA_INIT, "hpo_term_create_table.sql"), radiant_mapping))
    yield


@pytest.fixture(scope="session")
def supersession_fixtures(postgres_clinical_seeds, postgres_instance):
    """The shapes the seeds do not have: a shared experiment, a superseded one, a
    re-alignment, and a member still waiting to be sequenced.

    Documents are gVCFs only, and the annotation task carries none. Neither shape matches
    any branch of `staging_external_sequencing_experiment`, so none of this reaches the ETL
    staging views and no other integration test can see it.
    """
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
        cur.execute(
            """
            INSERT INTO patient (id, submitter_patient_id, organization_code, sex_code,
                                 date_of_birth, life_status_code, tenant_code)
            VALUES (9101, 'PT-9101', 'CQGC', 'female', '2015-01-01', 'alive', 'radiant'),
                   (9103, 'PT-9103', 'CQGC', 'female', '2016-01-01', 'alive', 'radiant'),
                   (9104, 'PT-9104', 'CQGC', 'male',   '2017-01-01', 'alive', 'radiant'),
                   (9105, 'PT-9105', 'CQGC', 'female', '2018-01-01', 'alive', 'radiant');

            INSERT INTO cases (id, proband_id, project_id, status_code, primary_condition,
                               created_on, updated_on, case_type_code, submitter_case_id, tenant_code)
            VALUES (9001, 9101, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'germline', 'SHARED-A', 'radiant'),
                   (9002, 9101, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'germline', 'SHARED-B', 'radiant'),
                   (9003, 9103, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'germline', 'SUPERSEDED', 'radiant'),
                   (9004, 9104, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'germline', 'REALIGNED', 'radiant'),
                   (9005, 9105, 1, 'in_progress', 'MONDO:0700092', now(), now(), 'germline', 'PENDING', 'radiant');

            INSERT INTO family (id, case_id, family_member_id, relationship_to_proband_code,
                                affected_status_code, tenant_code)
            VALUES (9001, 9001, 9101, 'proband', 'affected', 'radiant'),
                   -- The same patient, framed a second time: this is what a shared
                   -- sequencing experiment actually looks like in the model.
                   (9002, 9002, 9101, 'proband', 'affected', 'radiant'),
                   (9003, 9003, 9103, 'proband', 'affected', 'radiant'),
                   (9004, 9004, 9104, 'proband', 'affected', 'radiant'),
                   (9005, 9005, 9105, 'proband', 'affected', 'radiant'),
                   -- The pending case's second member is registered but never sequenced.
                   (9006, 9005, 9104, 'father',  'non_affected', 'radiant');

            INSERT INTO sample (id, type_code, histology_code, submitter_sample_id, patient_id,
                                organization_code, tenant_code)
            VALUES (9101, 'dna', 'normal', 'S9101', 9101, 'CQGC', 'radiant'),
                   (9103, 'dna', 'normal', 'S9103', 9103, 'CQGC', 'radiant'),
                   (9104, 'dna', 'normal', 'S9104', 9104, 'CQGC', 'radiant'),
                   (9105, 'dna', 'normal', 'S9105', 9105, 'CQGC', 'radiant');

            INSERT INTO sequencing_experiment (id, sample_id, status_code, aliquot, created_on,
                                               updated_on, experimental_strategy_code,
                                               platform_code, tenant_code)
            VALUES
                -- Shared: one experiment, two cases.
                (9101, 9101, 'completed', 'A9101', '2026-01-01', '2026-01-01', 'wgs', 'illumina', 'radiant'),
                -- Superseded: two completed experiments for one patient, the newer wins.
                (9103, 9103, 'completed', 'A9103-OLD', '2026-01-01', '2026-01-01', 'wgs', 'illumina', 'radiant'),
                (9113, 9103, 'completed', 'A9103-NEW', '2026-02-01', '2026-02-01', 'wgs', 'illumina', 'radiant'),
                -- Re-aligned: one experiment, two alignment tasks below.
                (9104, 9104, 'completed', 'A9104', '2026-01-01', '2026-01-01', 'wgs', 'illumina', 'radiant'),
                -- Pending: the proband is sequenced, the father is not.
                (9105, 9105, 'completed', 'A9105', '2026-01-01', '2026-01-01', 'wgs', 'illumina', 'radiant');

            INSERT INTO case_has_sequencing_experiment (case_id, sequencing_experiment_id)
            VALUES (9001, 9101), (9002, 9101),
                   (9003, 9103), (9003, 9113),
                   (9004, 9104),
                   (9005, 9105);

            INSERT INTO task (id, task_type_code, created_on, pipeline_name, pipeline_version,
                              genome_build, tenant_code)
            VALUES (9201, 'alignment_germline_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant'),
                   (9203, 'alignment_germline_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant'),
                   (9213, 'alignment_germline_variant_calling', '2026-02-02', 'Dragen', '4.4.4', 'GRch38', 'radiant'),
                   -- Two alignments over experiment 9104: the newer one wins.
                   (9204, 'alignment_germline_variant_calling', '2026-01-02', 'Dragen', '4.4.3', 'GRch38', 'radiant'),
                   (9214, 'alignment_germline_variant_calling', '2026-03-02', 'Dragen', '4.4.4', 'GRch38', 'radiant'),
                   (9205, 'alignment_germline_variant_calling', '2026-01-02', 'Dragen', '4.4.4', 'GRch38', 'radiant');

            INSERT INTO task_context (task_id, case_id, sequencing_experiment_id)
            VALUES (9201, null, 9101),
                   (9203, null, 9103),
                   (9213, null, 9113),
                   (9204, null, 9104),
                   (9214, null, 9104),
                   (9205, null, 9105);

            INSERT INTO document (id, name, data_category_code, data_type_code, format_code,
                                  size, url, tenant_code)
            VALUES (9301, 'a9101.gvcf.gz', 'genomic', 'snv', 'gvcf', 1, 's3://t/a9101.gvcf.gz', 'radiant'),
                   (9303, 'a9103old.gvcf.gz', 'genomic', 'snv', 'gvcf', 1, 's3://t/a9103old.gvcf.gz', 'radiant'),
                   (9313, 'a9103new.gvcf.gz', 'genomic', 'snv', 'gvcf', 1, 's3://t/a9103new.gvcf.gz', 'radiant'),
                   (9304, 'a9104old.gvcf.gz', 'genomic', 'snv', 'gvcf', 1, 's3://t/a9104old.gvcf.gz', 'radiant'),
                   (9314, 'a9104new.gvcf.gz', 'genomic', 'snv', 'gvcf', 1, 's3://t/a9104new.gvcf.gz', 'radiant'),
                   (9305, 'a9105.gvcf.gz', 'genomic', 'snv', 'gvcf', 1, 's3://t/a9105.gvcf.gz', 'radiant');

            INSERT INTO task_has_document (task_id, document_id, type)
            VALUES (9201, 9301, 'output'),
                   (9203, 9303, 'output'),
                   (9213, 9313, 'output'),
                   (9204, 9304, 'output'),
                   (9214, 9314, 'output'),
                   (9205, 9305, 'output');
            """
        )
        conn.commit()
    yield


# --- 1. happy path -------------------------------------------------------------------------


def test_a_trio_with_alignments_and_no_annotation_is_decided_run(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    rows = _facts(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert {r["patient_id"] for r in rows} == {44, 45, 46}
    assert all(r["tenant_code"] == "radiant" for r in rows)
    assert [r["gvcf_matches"] for r in rows] == [1, 1, 1]
    assert all(r["current_alignment_task_id"] for r in rows)
    assert not any(r["annotated"] for r in rows)
    assert _decided(rows) == {TRIO_CASE_ID: (Decision.RUN, ())}


def test_the_members_of_the_trio_are_what_the_pipeline_needs(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    rows = _members(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert {r["patient_id"] for r in rows} == {44, 45, 46}
    assert all(r["gvcf_url"] and r["aliquot"] and r["alignment_task_id"] for r in rows)


def test_the_family_is_ordered_proband_first_whatever_the_query_order(
    postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """The PED and phenopacket writers read members positionally: the family sorts them, not the query."""
    selection = select_run_cases(_members(starrocks_session, radiant_mapping, TRIO_CASE_ID), [])
    roles = [m.role for m in selection.members]
    assert roles == sorted(roles, key=lambda role: REL_ORDER[role])
    assert roles[0] == "proband"


def test_the_case_carries_a_project_code(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """`(project_code, submitter_case_id)` is what the batch PATCH looks a case up by, so a
    null here would fail registration long after the pipeline has run."""
    rows = _members(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert all(r["project_code"] for r in rows)
    assert all(r["submitter_case_id"] for r in rows)


# --- the anti-join --------------------------------------------------------------------------


def test_an_annotated_case_is_not_a_candidate(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """Case 8 already carries task 65, a `radiant_germline_annotation` over its experiment.
    Returning it would mean re-running the pipeline over it every night, for ever."""
    assert _facts(starrocks_session, radiant_mapping, ANNOTATED_CASE_ID) == []


def test_a_somatic_case_is_never_a_candidate(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """The pipeline's `step: genotype` entry point assumes germline joint calling."""
    assert _facts(starrocks_session, radiant_mapping, SOMATIC_CASE_ID) == []
    assert _members(starrocks_session, radiant_mapping, SOMATIC_CASE_ID) == []


def test_a_case_with_no_gvcfs_at_all_never_runs(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """Case 1 was joint-called upstream. A candidate now, documents or not, so the journal says why it does not
    run; it never runs."""
    rows = _facts(starrocks_session, radiant_mapping, 1)
    assert all(decision != Decision.RUN for decision, _ in _decided(rows).values())


# --- 3. one alignment task, several cases ---------------------------------------------------


def test_a_shared_experiment_yields_both_cases(
    supersession_fixtures, postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """Alignment tasks carry no case, so 'a task shared by two cases' is really one
    experiment linked to two. Both must come back, each as its own family."""
    decided = _decided(_facts(starrocks_session, radiant_mapping))
    assert decided[SHARED_CASE_A] == decided[SHARED_CASE_B] == (Decision.RUN, ())
    rows = _members(starrocks_session, radiant_mapping, SHARED_CASE_A, SHARED_CASE_B)
    assert {r["case_id"] for r in rows} == {SHARED_CASE_A, SHARED_CASE_B}
    # Same experiment, same gVCF, two cases -- which is correct: the pipeline joint-calls
    # each family independently and each case gets its own annotation task.
    assert {r["seq_id"] for r in rows} == {9101}
    assert {r["gvcf_url"] for r in rows} == {"s3://t/a9101.gvcf.gz"}


# --- 5. two experiments, no annotation ------------------------------------------------------


def test_the_newest_experiment_wins_and_the_older_one_disappears(
    supersession_fixtures, postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """The convergence test. The superseded experiment must be invisible: present, it would
    put the patient in the PED twice and -- because it has an alignment and no annotation --
    keep the case eligible for ever."""
    facts = _facts(starrocks_session, radiant_mapping, SUPERSEDED_CASE)
    assert [r["seq_id"] for r in facts] == [9113]
    assert _decided(facts) == {SUPERSEDED_CASE: (Decision.RUN, ())}
    rows = _members(starrocks_session, radiant_mapping, SUPERSEDED_CASE)
    assert len(rows) == 1
    assert rows[0]["seq_id"] == 9113
    assert rows[0]["aliquot"] == "A9103-NEW"
    assert rows[0]["gvcf_url"] == "s3://t/a9103new.gvcf.gz"


# --- 6. re-alignment ------------------------------------------------------------------------


def test_the_newest_alignment_task_wins(
    supersession_fixtures, postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """Two alignment tasks over one experiment used to give gvcf_matches = 2 and an error
    message blaming a mistyped document. The newest task is taken, and the count is now
    taken over that task alone -- so it means one thing."""
    facts = _facts(starrocks_session, radiant_mapping, REALIGNED_CASE)
    assert [r["current_alignment_task_id"] for r in facts] == [9214]
    assert [r["gvcf_matches"] for r in facts] == [1]
    rows = _members(starrocks_session, radiant_mapping, REALIGNED_CASE)
    assert len(rows) == 1
    assert rows[0]["alignment_task_id"] == 9214
    assert rows[0]["gvcf_url"] == "s3://t/a9104new.gvcf.gz"


def test_two_gvcfs_on_one_alignment_task_are_still_ambiguous(
    supersession_fixtures, postgres_clinical_seeds, starrocks_session, radiant_mapping, postgres_instance
):
    """The other half of the discrimination: one task cannot legitimately publish two gVCFs,
    so this can only be a document mistyped at the source."""
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
        cur.execute(
            """
            INSERT INTO document (id, name, data_category_code, data_type_code, format_code,
                                  size, url, tenant_code)
            VALUES (9399, 'a9104new.gvcf.gz.tbi', 'genomic', 'snv', 'gvcf', 1,
                    's3://t/a9104new.gvcf.gz.tbi', 'radiant');
            INSERT INTO task_has_document (task_id, document_id, type) VALUES (9214, 9399, 'output');
            """
        )
        conn.commit()
        try:
            facts = _facts(starrocks_session, radiant_mapping, REALIGNED_CASE)
            assert [r["gvcf_matches"] for r in facts] == [2]
            decision, reasons = _decided(facts)[REALIGNED_CASE]
            assert decision == Decision.BLOCKED
            assert [(r.code, r.detail) for r in reasons] == [(ReasonCode.AMBIGUOUS_INPUT, "gvcf")]
        finally:
            cur.execute("DELETE FROM task_has_document WHERE document_id = 9399;")
            cur.execute("DELETE FROM document WHERE id = 9399;")
            conn.commit()


# --- members that are not ready --------------------------------------------------------------


def test_a_father_never_sequenced_leaves_the_proband_to_run(
    supersession_fixtures, postgres_clinical_seeds, starrocks_session, radiant_mapping
):
    """Returned, not dropped: the father comes back with no experiment, and the journal names him. He does not hold
    the proband back; the family runs with the members present."""
    facts = _facts(starrocks_session, radiant_mapping, PENDING_CASE)
    by_role = {r["role"]: r for r in facts}
    assert by_role["father"]["seq_id"] is None
    father = by_role["father"]["patient_id"]
    assert _decided(facts) == {
        PENDING_CASE: (Decision.RUN, (Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=father),))
    }
    selection = select_run_cases(_members(starrocks_session, radiant_mapping, PENDING_CASE), [])
    assert selection.case_ids == [PENDING_CASE]
    assert [m.role for m in selection.members] == ["proband"]


def test_an_excluded_tenant_is_reported_not_hidden(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """Excluding it silently would leave someone wondering why the annotation never happened."""
    facts = _facts(starrocks_session, radiant_mapping, TRIO_CASE_ID)
    assert _decided(facts, ["radiant"]) == {TRIO_CASE_ID: (Decision.SKIP, (Reason(code=ReasonCode.TENANT_EXCLUDED),))}


# --- phenotypes --------------------------------------------------------------------------------


def test_phenotypes_come_back_for_the_proband(
    postgres_clinical_seeds, starrocks_session, radiant_mapping, hpo_term_table
):
    rows = _run(starrocks_session, radiant_mapping, PHENOTYPES, {"case_ids": [TRIO_CASE_ID]})
    proband_terms = [r for r in rows if r["patient_id"] == 44]
    assert proband_terms
    assert all(r["hpo_id"].startswith("HP:") for r in proband_terms)
    # Both interpretations are present in the seeds; `negative` becomes `excluded: true`.
    assert {r["interpretation_code"] for r in proband_terms} == {"positive", "negative"}


def test_an_hpo_code_the_dictionary_does_not_know_keeps_its_row(
    postgres_clinical_seeds, starrocks_session, radiant_mapping, hpo_term_table
):
    """A LEFT JOIN on purpose: a missing label is cosmetic, a dropped phenotype is not."""
    rows = _run(starrocks_session, radiant_mapping, PHENOTYPES, {"case_ids": [TRIO_CASE_ID]})
    # The dictionary is empty in this fixture, so every label is null and every row survives.
    assert rows
    assert all(r["hpo_label"] is None for r in rows)
