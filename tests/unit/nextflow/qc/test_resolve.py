import pytest

from radiant.tasks.nextflow.qc.resolve import CaseResolutionError, fold_rows, resolve_cases, select_cases, tenants_of

from .conftest import document_rows


def test_document_rows_fold_into_one_member_each(trio_rows):
    members = fold_rows(trio_rows)
    assert len(members) == 3
    proband = next(m for m in members if m.role == "proband")
    assert len(proband.document_urls) == 4
    assert proband.cram_url.endswith("NA12878.cram")
    assert proband.crai_url.endswith("NA12878.cram.crai")


def test_a_member_without_documents_still_folds(trio_rows):
    rows = trio_rows + document_rows(
        role="brother",
        patient_id=103,
        sex="male",
        aliquot=None,
        seq_id=None,
        alignment_task_id=None,
        exclusion_reason="pending_sequencing",
        documents=False,
    )
    members = fold_rows(rows)
    brother = next(m for m in members if m.role == "brother")
    assert brother.document_urls == []
    assert brother.exclusion_reason == "pending_sequencing"


def test_members_come_out_proband_first(trio_rows):
    selection = select_cases(trio_rows)
    assert [m.role for m in selection.members] == ["proband", "father", "mother"]
    assert selection.case_ids == [1072]


def test_a_case_with_an_excluded_member_is_dropped_whole_in_lenient_mode(trio_rows, singleton_rows):
    rows = trio_rows + singleton_rows
    rows += document_rows(
        role="sister",
        patient_id=104,
        aliquot=None,
        seq_id=None,
        alignment_task_id=None,
        exclusion_reason="pending_alignment",
        documents=False,
    )
    selection = select_cases(rows)
    assert selection.case_ids == [8]
    assert [e.reason for e in selection.excluded] == ["pending_alignment"]


def test_strict_mode_raises_instead(trio_rows):
    rows = trio_rows + document_rows(
        role="sister",
        patient_id=104,
        aliquot=None,
        seq_id=None,
        alignment_task_id=None,
        exclusion_reason="no_cram",
        documents=False,
    )
    with pytest.raises(CaseResolutionError, match="published no CRAM"):
        select_cases(rows, [900], strict=True)


def test_a_requested_task_with_no_candidate_is_reported(trio_rows):
    with pytest.raises(CaseResolutionError, match=r"\[999\]"):
        select_cases(trio_rows, [999])


def test_mixed_strategies_within_a_case_are_allowed(trio_rows):
    """Unlike post-processing: the QC samplesheet carries the strategy per row."""
    rows = [r | {"strategy": "wxs"} if r["role"] == "father" else r for r in trio_rows]
    assert select_cases(rows).case_ids == [1072]


def test_an_unknown_strategy_excludes(trio_rows):
    rows = [r | {"strategy": "rnas"} if r["role"] == "father" else r for r in trio_rows]
    selection = select_cases(rows)
    assert selection.case_ids == []
    assert selection.excluded[0].reason == "unsupported_strategy"


def test_two_probands_exclude(trio_rows):
    rows = [r | {"role": "proband"} if r["role"] == "father" else r for r in trio_rows]
    assert select_cases(rows).excluded[0].reason == "proband_count"


def test_resolve_builds_one_case_with_a_ca_family_id(trio_rows, singleton_rows):
    selection = select_cases(trio_rows + singleton_rows)
    cases = resolve_cases([m.model_dump() for m in selection.members])
    assert [c.family_id for c in cases] == ["CA8", "CA1072"]
    trio = cases[1]
    assert trio.proband.aliquot == "NA12878"
    assert trio.submitter_case_id == "1KGP-1463"
    assert trio.project_code == "N1"
    assert tenants_of(cases) == ["radiant"]
