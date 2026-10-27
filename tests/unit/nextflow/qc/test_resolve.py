import pytest

from radiant.tasks.nextflow.qc.resolve import CaseResolutionError, resolve_cases, select_cases, tenants_of

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
    assert [e.reason for e in selection.excluded] == ["pending_sequencing"]


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
    assert [e.reason for e in selection.excluded] == ["ambiguous_dragen_metrics"]
    assert "more than one DRAGEN metrics tarball" in selection.excluded[0].detail


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
    assert [e.reason for e in selection.excluded] == ["pending_alignment"]


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
