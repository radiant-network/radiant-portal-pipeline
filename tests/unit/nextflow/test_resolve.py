import pytest

from radiant.tasks.decision import Decision, Reason, ReasonCode
from radiant.tasks.nextflow.resolve import (
    CaseResolutionError,
    decide_cases,
    outcome_entries,
    resolve_families,
    select_cases,
    select_run_cases,
    tenants_of,
)
from tests.unit.nextflow.conftest import member_row


def _kept(rows, requested=None, **kwargs):
    """The rows `select_cases` would hand on, as dicts, ready for `resolve_families`."""
    selection = select_cases(rows, requested, **kwargs)
    return [m.model_dump() for m in selection.members]


# --- building a family -------------------------------------------------------------------


def test_members_are_ordered_proband_first(trio_rows, phenotype_rows):
    """The PED and phenopacket writers read members positionally: `members[0]` is the
    proband, and the parents are looked up by role."""
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert [m.role for m in family.members] == ["proband", "father", "mother"]
    assert family.proband.aliquot == "NA12878"
    assert family.father.aliquot == "NA12891"
    assert family.mother.aliquot == "NA12892"


def test_family_id_is_ca_plus_the_case_id(trio_rows, phenotype_rows):
    """Not the bare numeric id: nf-schema coerces that to a number and then fails its own
    `type: string` check with a misleading message about spaces."""
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert family.family_id == "CA1072"


def test_only_the_probands_phenotypes_are_kept(trio_rows, phenotype_rows):
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert [p.hpo_id for p in family.phenotypes] == ["HP:0001249", "HP:0000618"]


def test_strategy_maps_to_the_pipelines_vocabulary(singleton_rows):
    """`wxs` and `wes` are the same thing under two dictionaries; the pipeline knows WES."""
    (family,) = resolve_families(_kept(singleton_rows), [])
    assert family.sequencing_type == "WES"


# --- lenient: a discovered scope ---------------------------------------------------------


def test_a_discovered_case_that_cannot_run_is_excluded_not_raised(trio_rows, singleton_rows):
    """The whole point of automation: one unfixable case must not block every other case,
    every night."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    selection = select_cases(trio_rows + singleton_rows)
    assert selection.case_ids == [8]
    assert [(e.case_id, e.reason) for e in selection.excluded] == [
        (1072, Reason(code=ReasonCode.MISSING_INPUT, detail="gvcf", patient_id=102))
    ]


def test_the_exclusion_carries_a_sentence_not_only_a_code(trio_rows):
    """The report is read by someone who does not have the SQL open."""
    trio_rows[0]["exclusion_reason"] = "ambiguous_gvcf"
    (excluded,) = select_cases(trio_rows).excluded
    assert "mistyped at the source" in excluded.message
    assert "more than one gvcf document" in excluded.message


def test_one_bad_member_excludes_the_whole_case(trio_rows):
    """The pipeline joint-calls whole families, so a family missing a member is not a
    smaller job -- it is a different one."""
    trio_rows[1]["exclusion_reason"] = "pending_alignment"
    selection = select_cases(trio_rows)
    assert selection.case_ids == []
    assert selection.members == []


def test_a_member_awaiting_sequencing_makes_the_case_wait(trio_rows):
    """Rather than running against the superseded experiment and producing an annotation
    that is obsolete the moment it lands."""
    trio_rows[1].update(seq_id=None, sample_id=None, aliquot=None, strategy=None, gvcf_url=None)
    trio_rows[1]["exclusion_reason"] = "pending_sequencing"
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason.code == "pending_sequencing"
    assert "no completed sequencing experiment yet" in excluded.message


def test_a_case_without_exactly_one_proband_is_excluded(trio_rows):
    """After one experiment per member is selected this can only mean two different
    patients marked proband -- which is what the message now says."""
    trio_rows.append(member_row(role="proband", patient_id=103, sample_id="S12000", aliquot="NA12000"))
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason.code == "proband_count"
    assert "two different patients are marked proband" in excluded.message


def test_a_member_without_an_aliquot_is_excluded_by_name(trio_rows):
    """The aliquot is every id the pipeline sees. The query never leaves it null on a kept
    member today; if that changes, the case is excluded here rather than writing `None` into
    the PED and failing in Exomiser."""
    trio_rows[1]["aliquot"] = None
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason.code == "missing_aliquot"
    assert "patient 101" in excluded.message


def test_an_unsupported_strategy_is_excluded(singleton_rows):
    singleton_rows[0]["strategy"] = "rnaseq"
    (excluded,) = select_cases(singleton_rows).excluded
    assert excluded.reason.code == "unsupported_strategy"


def test_members_spanning_several_strategies_are_excluded(trio_rows):
    """A member whose newest experiment is a different strategy is not a re-sequencing."""
    trio_rows[0]["strategy"] = "wxs"
    (excluded,) = select_cases(trio_rows).excluded
    assert "span several strategies" in excluded.message


# --- strict: an operator named the tasks -------------------------------------------------


def test_a_named_task_that_cannot_run_raises(trio_rows):
    """An operator asked for this specific work, so silence would be the wrong answer."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    with pytest.raises(CaseResolutionError, match="published no gvcf document"):
        select_cases(trio_rows, [900, 901, 902])


def test_a_named_task_that_produced_no_candidate_is_reported(trio_rows):
    """The query filters candidacy by task id, so an id that comes back empty is either
    unknown, not an alignment task, or already annotated everywhere it applies."""
    with pytest.raises(CaseResolutionError, match=r"\[999\]"):
        select_cases(trio_rows, [900, 901, 902, 999])


def test_strict_reports_every_problem_at_once(trio_rows, singleton_rows):
    """One round trip per run, not one per defect: these queries are slow and the operator
    is remote."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    singleton_rows[0]["strategy"] = "rnaseq"
    with pytest.raises(CaseResolutionError) as excinfo:
        select_cases(trio_rows + singleton_rows, [900, 901, 902, 910, 999])
    message = str(excinfo.value)
    assert "published no gvcf document" in message
    assert "unsupported experimental strategy" in message
    assert "[999]" in message


def test_a_discovery_run_is_lenient_by_default(trio_rows):
    """No task ids means nobody asked for anything in particular."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    assert select_cases(trio_rows).excluded  # does not raise


# --- tenants ------------------------------------------------------------------------------


def test_the_tenant_is_read_off_the_cases(trio_rows, phenotype_rows):
    """`cases.id` is globally unique, so the tenant is a consequence of the cases rather
    than an input -- and a batch can only be addressed where its data came from."""
    families = resolve_families(_kept(trio_rows), phenotype_rows)
    assert families[0].tenant_code == "radiant"
    assert tenants_of(families) == ["radiant"]


def test_a_run_may_span_several_tenants(trio_rows, singleton_rows):
    """One batch PATCH per tenant, not one pipeline run per tenant. Splitting the run would
    serialise hours of WGS behind the pipeline DAG's max_active_runs=1 for no reason: reads
    are one shared schema and the pipeline itself is tenant-blind."""
    for row in singleton_rows:
        row["tenant_code"] = "other"
    selection = select_cases(trio_rows + singleton_rows)
    assert selection.case_ids == [8, 1072]
    assert selection.tenants == ["other", "radiant"]

    families = resolve_families([m.model_dump() for m in selection.members], [])
    assert tenants_of(families) == ["other", "radiant"]


def test_an_excluded_tenant_is_left_out_before_the_pipeline_runs(singleton_rows):
    """Reported with its reason, so the log says why the case never ran."""
    singleton_rows[0]["exclusion_reason"] = "tenant_excluded"
    (excluded,) = select_cases(singleton_rows).excluded
    assert excluded.reason.code == "tenant_excluded"
    assert "RADIANT_ETL_EXCLUDED_TENANTS" in excluded.message


# --- deciding from the facts -------------------------------------------------------------


def _facts(row, **overrides):
    """A facts row: no reason computed by the query, the current alignment carried instead."""
    facts = {k: v for k, v in row.items() if k != "exclusion_reason"}
    facts["current_alignment_task_id"] = row["alignment_task_id"]
    facts.update(overrides)
    return facts


def _decided(rows, excluded_tenants=()):
    return {e.case_id: (e.decision, e.reasons) for e in decide_cases(rows, list(excluded_tenants))}


PENDING_SEQUENCING = {"seq_id": None, "aliquot": None, "alignment_task_id": None, "gvcf_matches": 0}
NO_GVCF = {"alignment_task_id": None, "gvcf_url": None, "gvcf_matches": 0}


def test_a_ready_trio_runs_without_a_reason(trio_rows):
    assert _decided([_facts(r) for r in trio_rows]) == {1072: (Decision.RUN, ())}


def test_a_parent_not_sequenced_yet_does_not_hold_the_proband_back(trio_rows):
    rows = [_facts(r, **PENDING_SEQUENCING) if r["role"] == "father" else _facts(r) for r in trio_rows]
    assert _decided(rows) == {1072: (Decision.RUN, (Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101),))}


def test_a_parent_not_aligned_yet_does_not_hold_the_proband_back(trio_rows):
    pending_alignment = {"alignment_task_id": None, "current_alignment_task_id": None, "gvcf_matches": 0}
    rows = [_facts(r, **pending_alignment) if r["role"] == "mother" else _facts(r) for r in trio_rows]
    assert _decided(rows)[1072][0] == Decision.RUN


def test_after_the_partial_run_the_case_waits_for_the_absent_parent(trio_rows):
    rows = [_facts(r, **PENDING_SEQUENCING) if r["role"] == "father" else _facts(r, annotated=True) for r in trio_rows]
    assert _decided(rows) == {1072: (Decision.WAIT, (Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101),))}


def test_the_case_runs_again_once_the_parent_arrives(trio_rows):
    rows = [_facts(r) if r["role"] == "father" else _facts(r, annotated=True) for r in trio_rows]
    assert _decided(rows) == {1072: (Decision.RUN, ())}


def test_an_absent_proband_still_makes_the_case_wait(trio_rows):
    rows = [_facts(r, **PENDING_SEQUENCING) if r["role"] == "proband" else _facts(r) for r in trio_rows]
    assert _decided(rows) == {1072: (Decision.WAIT, (Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=100),))}


def test_a_parent_with_wrong_data_still_blocks(trio_rows):
    rows = [_facts(r, **NO_GVCF) if r["role"] == "father" else _facts(r) for r in trio_rows]
    assert _decided(rows)[1072] == (
        Decision.BLOCKED,
        (Reason(code=ReasonCode.MISSING_INPUT, detail="gvcf", patient_id=101),),
    )


def test_a_case_annotated_for_every_member_is_not_decided(trio_rows):
    assert _decided([_facts(r, annotated=True) for r in trio_rows]) == {}


def test_the_family_is_built_from_the_members_present(trio_rows, phenotype_rows):
    rows = [_facts(r, **PENDING_SEQUENCING) if r["role"] == "father" else _facts(r) for r in trio_rows]
    selection = select_run_cases(rows, [])
    assert selection.case_ids == [1072]
    (family,) = resolve_families([m.model_dump() for m in selection.members], phenotype_rows)
    assert [m.role for m in family.members] == ["proband", "mother"]
    assert family.father is None


def test_a_blocking_reason_wins_over_a_waiting_one_and_both_are_kept(trio_rows):
    by_role = {"proband": NO_GVCF, "father": PENDING_SEQUENCING}
    rows = [_facts(r, **by_role.get(r["role"], {})) for r in trio_rows]
    assert _decided(rows) == {
        1072: (
            Decision.BLOCKED,
            (
                Reason(code=ReasonCode.MISSING_INPUT, detail="gvcf", patient_id=100),
                Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101),
            ),
        )
    }


def test_an_excluded_tenant_is_a_skip_even_while_a_member_waits(trio_rows):
    rows = [_facts(r, **PENDING_SEQUENCING) if r["role"] == "father" else _facts(r) for r in trio_rows]
    decision, reasons = _decided(rows, ["radiant"])[1072]
    assert decision == Decision.SKIP
    assert set(reasons) == {
        Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101),
        Reason(code=ReasonCode.TENANT_EXCLUDED),
    }


def test_a_member_without_an_aliquot_blocks_the_case(trio_rows):
    rows = [_facts(r, aliquot=None) if r["role"] == "mother" else _facts(r) for r in trio_rows]
    assert _decided(rows)[1072] == (Decision.BLOCKED, (Reason(code=ReasonCode.MISSING_ALIQUOT, patient_id=102),))


def test_a_case_with_no_experiment_yet_waits(singleton_rows):
    rows = [_facts(r, **PENDING_SEQUENCING) for r in singleton_rows]
    assert _decided(rows)[8] == (Decision.WAIT, (Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=200),))


# Each case: what the facts say, and the reason the pending query gives the same member.
SCENARIOS = [
    ({}, None),
    (PENDING_SEQUENCING, "pending_sequencing"),
    ({"alignment_task_id": None, "current_alignment_task_id": None, "gvcf_matches": 0}, "pending_alignment"),
    (NO_GVCF, "no_gvcf"),
    ({"gvcf_matches": 2}, "ambiguous_gvcf"),
    ({"project_code": None}, "no_project_code"),
    ({"tenant_code": "excluded"}, "tenant_excluded"),
    ({"aliquot": None}, None),
    ({"strategy": "rnaseq"}, None),
]


def test_the_facts_run_exactly_the_cases_the_pending_query_selects():
    facts, pending = [], []
    for case_id, (overrides, query_reason) in enumerate(SCENARIOS, start=1):
        row = member_row(case_id=case_id, **{k: v for k, v in overrides.items() if k != "current_alignment_task_id"})
        facts.append(_facts(row, **overrides))
        pending.append(row | {"exclusion_reason": query_reason})
    ran = {case_id for case_id, (decision, _) in _decided(facts, ["excluded"]).items() if decision == Decision.RUN}
    assert ran == set(select_cases(pending, strict=False).case_ids) == {1}


def test_two_members_waiting_are_two_reasons(trio_rows):
    rows = [_facts(r, **PENDING_SEQUENCING) if r["role"] != "proband" else _facts(r) for r in trio_rows]
    assert set(_decided(rows)[1072][1]) == {
        Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101),
        Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=102),
    }


def test_a_case_level_reason_names_no_member(trio_rows):
    rows = [_facts(r, project_code=None) for r in trio_rows]
    assert _decided(rows)[1072] == (Decision.BLOCKED, (Reason(code=ReasonCode.NO_PROJECT_CODE),))


def test_the_child_selects_from_the_facts_like_the_scheduler_decides():
    facts = [
        _facts(member_row(case_id=case_id, **{k: v for k, v in o.items() if k != "current_alignment_task_id"}), **o)
        for case_id, (o, _) in enumerate(SCENARIOS, start=1)
    ]
    selected = select_run_cases(facts, ["excluded"])
    ran = {case_id for case_id, (decision, _) in _decided(facts, ["excluded"]).items() if decision == Decision.RUN}
    assert set(selected.case_ids) == ran == {1}
    assert {e.case_id for e in selected.excluded} == set(range(2, len(SCENARIOS) + 1))


def _member(row, **overrides):
    """A row of `germline_snv_case_members_select.sql`: no reason, no current alignment of its own."""
    return {k: v for k, v in row.items() if k != "exclusion_reason"} | overrides


def test_the_child_leaves_out_a_parent_with_no_gvcf_yet(trio_rows):
    rows = [_member(r, **NO_GVCF) if r["role"] == "father" else _member(r) for r in trio_rows]
    selection = select_run_cases(rows, [])
    assert selection.case_ids == [1072]
    assert sorted(m.role for m in selection.members) == ["mother", "proband"]


def test_the_child_excludes_a_case_whose_proband_has_no_gvcf(trio_rows):
    rows = [_member(r, **NO_GVCF) if r["role"] == "proband" else _member(r) for r in trio_rows]
    selection = select_run_cases(rows, [])
    assert selection.case_ids == []
    assert [e.case_id for e in selection.excluded] == [1072]


def test_the_child_excludes_a_case_with_two_gvcfs_for_a_member(trio_rows):
    rows = [_member(r, gvcf_matches=2) if r["role"] == "mother" else _member(r) for r in trio_rows]
    assert select_run_cases(rows, []).case_ids == []


def test_outcomes_follow_the_registration_of_each_tenant():
    selection = {
        "case_ids": [1, 2],
        "members": [{"case_id": 1, "tenant_code": "a"}, {"case_id": 2, "tenant_code": "b"}],
        "excluded": [
            {"case_id": 3, "reason": {"code": "ambiguous_input", "detail": "gvcf", "patient_id": 7}, "message": "x"}
        ],
    }
    assert [(e.case_id, e.decision, e.reasons) for e in outcome_entries(selection, {"a"})] == [
        (1, Decision.SUCCEEDED, ()),
        (2, Decision.FAILED, ()),
        (3, Decision.FAILED, (Reason(code=ReasonCode.AMBIGUOUS_INPUT, detail="gvcf", patient_id=7),)),
    ]


def test_no_selection_journals_nothing():
    assert outcome_entries({}, set()) == []
