import logging

import pytest

from radiant.tasks.decision import Decision, Reason, ReasonCode
from radiant.tasks.journal import Pipeline
from radiant.tasks.nextflow.qc.resolve import (
    CaseResolutionError,
    decide_cases,
    decide_somatic_cases,
    resolve_cases,
    select_cases,
    select_run_cases,
    tenants_of,
)

from .conftest import member_row


def test_each_row_is_one_member(trio_rows):
    proband = select_cases(trio_rows).members[0]
    assert proband.cram_url.endswith("NA12878.cram")
    assert proband.crai_url.endswith("NA12878.cram.crai")
    assert proband.dragen_metrics_url.endswith("NA12878.extra.tgz")


def test_a_member_without_documents_still_comes_back(trio_rows):
    rows = trio_rows + [
        member_row(
            role="brother",
            patient_id=103,
            sex="male",
            aliquot=None,
            seq_id=None,
            alignment_task_id=None,
            exclusion_reason="pending_sequencing",
            documents=False,
        )
    ]
    selection = select_cases(rows)
    assert selection.case_ids == []
    assert [e.reason.code for e in selection.excluded] == ["pending_sequencing"]


def test_members_come_out_proband_first(trio_rows):
    selection = select_cases(trio_rows)
    assert [m.role for m in selection.members] == ["proband", "father", "mother"]
    assert selection.case_ids == [1072]


def test_a_member_without_a_tarball_still_runs(singleton_rows):
    """The samplesheet falls back to the CRAM's directory: nothing to exclude."""
    selection = select_cases(singleton_rows)
    assert selection.case_ids == [8]
    assert selection.members[0].dragen_metrics_url is None


def test_two_tarballs_exclude_the_case(trio_rows, singleton_rows):
    ambiguous = {"dragen_metrics_matches": 2, "exclusion_reason": "ambiguous_dragen_metrics"}
    rows = [r | ambiguous if r["role"] == "father" else r for r in trio_rows]
    selection = select_cases(rows + singleton_rows)
    assert selection.case_ids == [8]
    assert [e.reason for e in selection.excluded] == [
        Reason(code=ReasonCode.AMBIGUOUS_INPUT, detail="metrics", patient_id=101)
    ]
    assert "more than one metrics document" in selection.excluded[0].message


def test_a_case_with_an_excluded_member_is_dropped_whole_in_lenient_mode(trio_rows, singleton_rows):
    rows = trio_rows + singleton_rows
    rows += [
        member_row(
            role="sister",
            patient_id=104,
            aliquot=None,
            seq_id=None,
            alignment_task_id=None,
            exclusion_reason="pending_alignment",
            documents=False,
        )
    ]
    selection = select_cases(rows)
    assert selection.case_ids == [8]
    assert [e.reason.code for e in selection.excluded] == ["pending_alignment"]


def test_strict_mode_raises_instead(trio_rows):
    rows = trio_rows + [
        member_row(
            role="sister",
            patient_id=104,
            aliquot=None,
            seq_id=None,
            alignment_task_id=None,
            exclusion_reason="no_cram",
            documents=False,
        )
    ]
    with pytest.raises(CaseResolutionError, match="has no cram: a document"):
        select_cases(rows, [900], strict=True)


def test_a_requested_task_with_no_candidate_is_reported(trio_rows):
    with pytest.raises(CaseResolutionError, match=r"\[999\]") as error:
        select_cases(trio_rows, [999])
    assert "`alignment_germline_variant_calling`" in str(error.value)
    assert "`alignment_somatic_variant_calling`" in str(error.value)


def test_mixed_strategies_within_a_case_are_allowed(trio_rows):
    """Unlike post-processing: the QC samplesheet carries the strategy per row."""
    rows = [r | {"strategy": "wxs"} if r["role"] == "father" else r for r in trio_rows]
    assert select_cases(rows).case_ids == [1072]


def test_an_unknown_strategy_excludes(trio_rows):
    rows = [r | {"strategy": "rnas"} if r["role"] == "father" else r for r in trio_rows]
    selection = select_cases(rows)
    assert selection.case_ids == []
    assert selection.excluded[0].reason.code == "unsupported_strategy"


def test_two_probands_exclude(trio_rows):
    rows = [r | {"role": "proband"} if r["role"] == "father" else r for r in trio_rows]
    assert select_cases(rows).excluded[0].reason.code == "proband_count"


def test_resolve_builds_one_case_with_a_ca_family_id(trio_rows, singleton_rows):
    selection = select_cases(trio_rows + singleton_rows)
    cases = resolve_cases([m.model_dump() for m in selection.members])
    assert [c.family_id for c in cases] == ["CA8", "CA1072"]
    trio = cases[1]
    assert trio.proband.aliquot == "NA12878"
    assert trio.submitter_case_id == "1KGP-1463"
    assert trio.project_code == "N1"
    assert tenants_of(cases) == ["radiant"]


def test_a_tumor_only_case_runs_on_its_tumor_alone(tumor_rows, trio_rows):
    selection = select_cases(trio_rows + tumor_rows)
    assert selection.case_ids == [23, 1072]
    tumor = resolve_cases([m.model_dump() for m in selection.members])[0]
    assert (tumor.family_id, tumor.case_type, tumor.is_somatic) == ("CA23", "somatic", True)
    assert [(m.role, m.aliquot) for m in tumor.members] == [("proband", "TCRBOA6-T")]


def test_a_tumor_needs_no_cram(tumor_rows):
    """The pipeline never reads a tumor's CRAM: a member with a tarball and no CRAM is valid."""
    assert tumor_rows[0]["cram_url"] is None and tumor_rows[0]["dragen_metrics_url"]
    assert select_cases(tumor_rows).case_ids == [23]


def test_a_germline_member_without_a_cram_is_still_excluded(trio_rows):
    rows = [
        r | {"cram_url": None, "crai_url": None, "cram_matches": 0, "exclusion_reason": "no_cram"} for r in trio_rows
    ]
    selection = select_cases(rows)
    assert selection.case_ids == []
    assert {e.reason for e in selection.excluded} == {
        Reason(code=ReasonCode.MISSING_INPUT, detail="cram", patient_id=100)
    }


def test_a_tumor_with_two_tarballs_is_excluded_as_ambiguous(tumor_rows):
    rows = [r | {"dragen_metrics_matches": 2, "exclusion_reason": "ambiguous_dragen_metrics"} for r in tumor_rows]
    selection = select_cases(rows)
    assert selection.case_ids == []
    assert [(e.case_id, e.reason) for e in selection.excluded] == [
        (23, Reason(code=ReasonCode.AMBIGUOUS_INPUT, detail="metrics", patient_id=62))
    ]


def test_a_tumor_without_a_tarball_is_excluded_as_no_metrics(tumor_rows, trio_rows, caplog):
    """No CRAM-directory fallback for a tumor: it would give a green run with an empty report."""
    rows = [r | {"dragen_metrics_url": None, "exclusion_reason": "no_metrics"} for r in tumor_rows]
    with caplog.at_level(logging.WARNING):
        selection = select_cases(rows + trio_rows)
    assert selection.case_ids == [1072]
    assert [(e.case_id, e.reason) for e in selection.excluded] == [
        (23, Reason(code=ReasonCode.MISSING_INPUT, detail="metrics", patient_id=62))
    ]
    assert "case 23 excluded (missing_input:metrics)" in caplog.text
    assert "has no metrics: a document" in caplog.text


def test_a_targeted_rerun_of_a_tumor_without_a_tarball_fails(tumor_rows):
    rows = [r | {"dragen_metrics_url": None, "exclusion_reason": "no_metrics"} for r in tumor_rows]
    with pytest.raises(CaseResolutionError, match="has no metrics: a document"):
        select_cases(rows, [76])


def test_a_germline_case_keeps_its_type(trio_rows):
    case = resolve_cases([m.model_dump() for m in select_cases(trio_rows).members])[0]
    assert (case.case_type, case.is_somatic) == ("germline", False)


# --- the scheduler's decision -------------------------------------------------------------


_NOT_SEQUENCED = {
    "seq_id": None,
    "aliquot": None,
    "alignment_task_id": None,
    "current_alignment_task_id": None,
    "cram_url": None,
    "crai_url": None,
    "cram_matches": 0,
    "dragen_metrics_url": None,
    "dragen_metrics_matches": 0,
}


def _father(rows):
    return next(row for row in rows if row["role"] == "father")


def _decided(rows, excluded_tenants=(), decide=decide_cases):
    return [(e.case_id, e.decision, e.reasons) for e in decide(rows, list(excluded_tenants))]


def _first_run(*patient_ids):
    return tuple(Reason(code=ReasonCode.FIRST_RUN, patient_id=p) for p in patient_ids)


def _new_sequencing(*patient_ids):
    return tuple(Reason(code=ReasonCode.NEW_SEQUENCING, patient_id=p) for p in patient_ids)


def _qc_before(rows, *not_qc_roles):
    for row in rows:
        row["case_annotated"] = True
        row["annotated"] = row["role"] not in not_qc_roles


def test_a_first_run_names_every_member(trio_rows):
    (entry,) = decide_cases(trio_rows, [])
    assert (entry.pipeline, entry.decision, entry.reasons) == (
        Pipeline.QUALITY_CONTROL,
        Decision.RUN,
        _first_run(100, 101, 102),
    )


def test_a_parent_not_sequenced_yet_does_not_block_the_proband(trio_rows):
    _father(trio_rows).update(_NOT_SEQUENCED)
    assert _decided(trio_rows) == [
        (1072, Decision.RUN, (*_first_run(100, 102), Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101)))
    ]


def test_after_the_partial_run_the_case_waits_for_the_absent_parent(trio_rows):
    _father(trio_rows).update(_NOT_SEQUENCED)
    _qc_before(trio_rows, "father")
    assert _decided(trio_rows) == [
        (1072, Decision.WAIT, (Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101),))
    ]


def test_the_case_runs_again_once_the_parent_arrives_and_says_for_whom(trio_rows):
    _qc_before(trio_rows, "father")
    assert _decided(trio_rows) == [(1072, Decision.RUN, _new_sequencing(101))]


def test_a_singleton_sequenced_again_is_a_new_sequencing(singleton_rows):
    _qc_before(singleton_rows, "proband")
    assert _decided(singleton_rows) == [(8, Decision.RUN, _new_sequencing(200))]


def test_a_member_without_a_cram_blocks_the_case(trio_rows):
    _father(trio_rows).update(cram_url=None, cram_matches=0)
    assert _decided(trio_rows) == [
        (1072, Decision.BLOCKED, (Reason(code=ReasonCode.MISSING_INPUT, detail="cram", patient_id=101),))
    ]


def test_two_tarballs_block_a_germline_case(trio_rows):
    _father(trio_rows)["dragen_metrics_matches"] = 2
    assert _decided(trio_rows) == [
        (1072, Decision.BLOCKED, (Reason(code=ReasonCode.AMBIGUOUS_INPUT, detail="metrics", patient_id=101),))
    ]


def test_a_germline_member_without_a_tarball_still_runs(singleton_rows):
    assert _decided(singleton_rows) == [(8, Decision.RUN, _first_run(200))]


def test_an_excluded_tenant_is_skipped(trio_rows):
    (entry,) = decide_cases(trio_rows, ["radiant"])
    assert entry.decision == Decision.SKIP


def test_a_somatic_first_run_names_the_tumor(tumor_rows):
    (entry,) = decide_somatic_cases(tumor_rows, [])
    assert (entry.pipeline, entry.decision, entry.reasons) == (
        Pipeline.QUALITY_CONTROL_SOMATIC,
        Decision.RUN,
        _first_run(62),
    )


def test_a_tumor_sequenced_again_is_a_new_sequencing(tumor_rows):
    tumor_rows[0]["case_annotated"] = True
    assert _decided(tumor_rows, decide=decide_somatic_cases) == [(23, Decision.RUN, _new_sequencing(62))]


def test_a_tumor_without_a_tarball_blocks_the_case(tumor_rows):
    tumor_rows[0].update(dragen_metrics_url=None, dragen_metrics_matches=0)
    assert _decided(tumor_rows, decide=decide_somatic_cases) == [
        (23, Decision.BLOCKED, (Reason(code=ReasonCode.MISSING_INPUT, detail="metrics", patient_id=62),))
    ]


# --- the child's check of the named cases ---------------------------------------------------


def test_the_child_runs_the_family_without_its_absent_parent(trio_rows):
    _father(trio_rows).update(_NOT_SEQUENCED)
    selection = select_run_cases(trio_rows, [])
    assert selection.case_ids == [1072]
    assert [m.role for m in selection.members] == ["proband", "mother"]


def test_the_child_leaves_out_a_case_whose_proband_is_absent(trio_rows):
    next(r for r in trio_rows if r["role"] == "proband").update(_NOT_SEQUENCED)
    selection = select_run_cases(trio_rows, [])
    assert selection.case_ids == []
    assert [e.reason.code for e in selection.excluded] == ["pending_sequencing"]


def test_the_child_skips_an_excluded_tenant(tumor_rows):
    selection = select_run_cases(tumor_rows, ["radiant"])
    assert selection.case_ids == []
    assert [e.reason.code for e in selection.excluded] == ["tenant_excluded"]
