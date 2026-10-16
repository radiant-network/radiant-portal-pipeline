import pytest

from radiant.tasks.nextflow.somatic.resolve import CaseResolutionError, select_tasks
from tests.unit.nextflow.somatic.conftest import calling_task_rows, row, vcf_url


def test_a_tumor_normal_and_a_tumor_only_call_are_both_selected():
    rows = calling_task_rows(task_id=75, case_id=22, normal=True) + calling_task_rows(task_id=76, case_id=23)
    selection = select_tasks(rows)

    assert [t.task_id for t in selection.tasks] == [75, 76]
    assert selection.case_ids == [22, 23]
    assert selection.excluded == []
    tn, to = selection.tasks
    assert tn.aliquots == ["TUMOR-1", "NORMAL-1"]
    assert to.aliquots == ["TUMOR-1"]
    assert tn.vcf_url == vcf_url(75)
    assert tn.family_id == "75"
    assert tn.sequencing_type == "WGS"


def test_tumor_comes_first_whatever_the_row_order():
    rows = list(reversed(calling_task_rows(task_id=75, normal=True)))
    assert select_tasks(rows).tasks[0].aliquots == ["TUMOR-1", "NORMAL-1"]


def test_wxs_is_handed_to_the_pipeline_as_wes():
    assert select_tasks(calling_task_rows(strategy="wxs")).tasks[0].sequencing_type == "WES"


def test_a_task_held_by_two_cases_runs_once_for_both():
    rows = calling_task_rows(task_id=70, case_id=23) + calling_task_rows(task_id=70, case_id=24)
    selection = select_tasks(rows)
    assert len(selection.tasks) == 1
    assert [c.case_id for c in selection.tasks[0].cases] == [23, 24]
    assert selection.case_ids == [23, 24]


@pytest.mark.parametrize("reason", ["pending_sequencing", "no_ssnv", "ambiguous_ssnv"])
def test_a_task_level_reason_excludes_the_task_from_every_case(reason):
    rows = calling_task_rows(case_id=23, exclusion_reason=reason) + calling_task_rows(
        case_id=24, exclusion_reason=reason
    )
    selection = select_tasks(rows)
    assert selection.tasks == []
    assert selection.case_ids == []
    [excluded] = selection.excluded
    assert (excluded.task_id, excluded.case_ids, excluded.reason) == (70, [23, 24], reason)
    assert "calling task 70" in excluded.detail


@pytest.mark.parametrize("reason", ["no_project_code", "tenant_excluded"])
def test_a_case_level_reason_drops_that_case_only(reason):
    rows = calling_task_rows(case_id=23) + calling_task_rows(case_id=24, exclusion_reason=reason)
    selection = select_tasks(rows)
    assert [c.case_id for c in selection.tasks[0].cases] == [23]
    [excluded] = selection.excluded
    assert (excluded.case_ids, excluded.reason) == ([24], reason)


def test_a_task_with_its_only_case_dropped_does_not_run():
    selection = select_tasks(calling_task_rows(exclusion_reason="tenant_excluded"))
    assert selection.tasks == []
    assert selection.excluded[0].reason == "tenant_excluded"


def test_an_alignment_call_is_always_tumor_only():
    rows = calling_task_rows(task_id=76, task_type="alignment_somatic_variant_calling")
    task = select_tasks(rows).tasks[0]
    assert task.aliquots == ["TUMOR-1"]
    assert task.task_type == "alignment_somatic_variant_calling"


def test_an_alignment_call_with_two_aliquots_is_excluded():
    rows = [
        row(task_id=76, task_type="alignment_somatic_variant_calling"),
        row(task_id=76, task_type="alignment_somatic_variant_calling", seq_id=63, aliquot="N", histology="normal"),
    ]
    assert select_tasks(rows).excluded[0].reason == "unsupported_samples"


@pytest.mark.parametrize(
    "histologies",
    [["normal"], ["tumoral", "tumoral"], ["tumoral", "normal", "normal"], [None]],
)
def test_anything_but_one_tumor_plus_at_most_one_normal_is_excluded(histologies):
    rows = [row(seq_id=60 + i, aliquot=f"A{i}", histology=h) for i, h in enumerate(histologies)]
    assert select_tasks(rows).excluded[0].reason == "unsupported_samples"


def test_a_missing_aliquot_is_excluded():
    assert select_tasks(calling_task_rows(aliquot=None)).excluded[0].reason == "missing_aliquot"


@pytest.mark.parametrize("strategy", ["rna", None])
def test_an_unsupported_strategy_is_excluded(strategy):
    assert select_tasks(calling_task_rows(strategy=strategy)).excluded[0].reason == "unsupported_strategy"


def test_lenient_mode_runs_the_rest():
    rows = calling_task_rows(task_id=70) + calling_task_rows(task_id=71, case_id=24, exclusion_reason="no_ssnv")
    selection = select_tasks(rows, strict=False)
    assert [t.task_id for t in selection.tasks] == [70]


def test_a_named_task_that_cannot_run_is_an_error_naming_it():
    with pytest.raises(CaseResolutionError, match="calling task 71"):
        select_tasks(calling_task_rows(task_id=71, exclusion_reason="no_ssnv"), [71])


def test_a_named_task_that_produced_no_row_is_an_error():
    with pytest.raises(CaseResolutionError, match=r"\[99\]"):
        select_tasks(calling_task_rows(task_id=70), [70, 99])


def test_tenants_are_read_off_the_cases():
    rows = calling_task_rows(case_id=23) + calling_task_rows(task_id=71, case_id=24, tenant_code="other")
    assert select_tasks(rows).tenants == ["other", "radiant"]
