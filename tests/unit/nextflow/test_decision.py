"""The decision log rows: what each selection decided, per unit and per member at fault."""

from types import SimpleNamespace

import pytest

from radiant.tasks.nextflow import decision
from radiant.tasks.nextflow.cnv.resolve import REASON_TEXT as CNV_REASONS
from radiant.tasks.nextflow.decision import (
    EXCLUDED,
    GERMLINE_SNV,
    NOT_DECIDED,
    RUN,
    SKIPPED,
    SOMATIC_SNV,
    WAIT,
    case_decision,
    decide,
    decision_of,
    germline_decisions,
    import_decisions,
    keep_decided,
    narrow,
    run_case_ids,
    run_units,
    somatic_decisions,
)
from radiant.tasks.nextflow.problem import Problem
from radiant.tasks.nextflow.qc.resolve import REASON_TEXT as QC_REASONS
from radiant.tasks.nextflow.resolve import REASON_TEXT as SNV_REASONS
from radiant.tasks.nextflow.resolve import select_cases as select_snv
from radiant.tasks.nextflow.somatic.resolve import CASE_REASON_TEXT, TASK_REASON_TEXT
from radiant.tasks.nextflow.somatic.resolve import select_tasks as select_somatic
from tests.unit.nextflow.conftest import member_row as snv_member_row
from tests.unit.nextflow.qc.conftest import member_row as qc_member_row
from tests.unit.nextflow.somatic.conftest import calling_task_rows

PYTHON_REASONS = {
    "missing_aliquot",
    "proband_count",
    "unsupported_strategy",
    "unknown_caller",
    "unsupported_caller",
    "unsupported_samples",
}
EVERY_REASON = (
    set(SNV_REASONS) | set(CNV_REASONS) | set(QC_REASONS) | set(TASK_REASON_TEXT) | set(CASE_REASON_TEXT)
) | PYTHON_REASONS


def trio(**by_role) -> list[dict]:
    """Case 1072: proband 100, father 101, mother 102, each with its own experiment and alignment.
    `by_role` overrides a member's row, e.g. `father={"exclusion_reason": "pending_alignment"}`."""
    rows = []
    for index, role in enumerate(("proband", "father", "mother")):
        overrides = {
            "role": role,
            "patient_id": 100 + index,
            "seq_id": 500 + index,
            "alignment_task_id": 900 + index,
            "aliquot": f"AL-{index}",
            **by_role.get(role, {}),
        }
        rows.append(snv_member_row(**overrides))
    return rows


def snv(rows: list[dict]) -> list[decision.Decision]:
    return germline_decisions(GERMLINE_SNV, rows, select_snv(rows, strict=False))


def summary(decisions) -> list[tuple]:
    return [(d.scope, d.case_id, d.patient_id, d.seq_id, d.task_id, d.decision, d.reason) for d in decisions]


@pytest.mark.parametrize("reason", sorted(EVERY_REASON))
def test_every_reason_code_maps_to_one_decision(reason):
    expected = {"pending_sequencing": WAIT, "pending_alignment": WAIT, "tenant_excluded": SKIPPED}.get(
        reason, EXCLUDED
    )
    assert decision_of(reason) == expected


def test_no_reason_is_run():
    assert decision_of(None) == RUN


@pytest.mark.parametrize(
    ("reasons", "expected"),
    [
        ([], (RUN, None)),
        (["pending_alignment"], (WAIT, "pending_alignment")),
        (["tenant_excluded", "pending_sequencing"], (SKIPPED, "tenant_excluded")),
        (["pending_alignment", "ambiguous_gvcf"], (EXCLUDED, "ambiguous_gvcf")),
        (["pending_alignment", "tenant_excluded", "no_gvcf"], (EXCLUDED, "no_gvcf")),
        (["no_gvcf", "ambiguous_gvcf"], (EXCLUDED, "no_gvcf")),
    ],
)
def test_the_case_takes_the_most_severe_decision_and_its_first_reason(reasons, expected):
    problems = [Problem(reason=reason, detail=reason) for reason in reasons]
    assert case_decision(problems)[:2] == expected


def test_a_runnable_case_is_one_case_row():
    assert summary(snv(trio())) == [("case", 1072, None, None, None, RUN, None)]


def test_two_members_at_fault_give_one_case_row_and_one_row_each():
    """Today's warning names the father (first problem); the case row names the most severe."""
    rows = trio(
        father={"exclusion_reason": "pending_alignment", "alignment_task_id": None},
        mother={"exclusion_reason": "ambiguous_gvcf"},
    )
    assert summary(snv(rows)) == [
        ("case", 1072, None, None, None, EXCLUDED, "ambiguous_gvcf"),
        ("member", 1072, 101, 501, None, WAIT, "pending_alignment"),
        ("member", 1072, 102, 502, 902, EXCLUDED, "ambiguous_gvcf"),
    ]


def test_a_member_never_sequenced_has_no_experiment():
    rows = trio(mother={"exclusion_reason": "pending_sequencing", "seq_id": None, "alignment_task_id": None})
    assert summary(snv(rows))[1] == ("member", 1072, 102, None, None, WAIT, "pending_sequencing")


@pytest.mark.parametrize("reason", ["no_project_code", "tenant_excluded"])
def test_a_case_level_reason_repeated_on_every_member_gives_no_member_row(reason):
    rows = trio(**{role: {"exclusion_reason": reason} for role in ("proband", "father", "mother")})
    assert summary(snv(rows)) == [("case", 1072, None, None, None, decision_of(reason), reason)]


def test_a_python_reason_about_the_case_gives_no_member_row():
    rows = trio(father={"role": "proband"})
    assert summary(snv(rows)) == [("case", 1072, None, None, None, EXCLUDED, "proband_count")]


def test_a_member_without_aliquot_is_named():
    rows = trio(mother={"aliquot": None})
    assert summary(snv(rows))[1] == ("member", 1072, 102, 502, 902, EXCLUDED, "missing_aliquot")


def test_the_tenant_comes_from_the_rows():
    rows = trio(proband={"exclusion_reason": "no_gvcf"})
    assert {d.tenant_code for d in snv(rows)} == {"radiant"}


def test_qc_decides_germline_and_somatic_cases_as_one_pipeline():
    rows = [qc_member_row(case_id=3), qc_member_row(case_id=23, case_type="somatic", affected_status="unknown")]
    decisions = decide([], [], [], rows, [])
    assert [(d.pipeline, d.case_id, d.decision) for d in decisions] == [("qc", 3, RUN), ("qc", 23, RUN)]


def test_somatic_units_are_calling_task_and_case_pairs():
    """A case with a tumor-normal call that runs and a tumor-only call without VCF: two units."""
    rows = [*calling_task_rows(task_id=70, case_id=22, normal=True), *calling_task_rows(task_id=71, case_id=22)]
    rows[-1]["exclusion_reason"] = "no_ssnv"
    decisions = somatic_decisions(rows, select_somatic(rows, strict=False))
    assert [(d.scope, d.unit_id, d.case_id, d.decision, d.reason) for d in decisions] == [
        ("case", 70, 22, RUN, None),
        ("case", 71, 22, EXCLUDED, "no_ssnv"),
    ]


def test_a_somatic_task_excluded_for_two_cases_gives_two_case_rows():
    rows = [*calling_task_rows(task_id=70, case_id=22), *calling_task_rows(task_id=70, case_id=30)]
    for row in rows:
        row["exclusion_reason"] = "ambiguous_ssnv"
    decisions = somatic_decisions(rows, select_somatic(rows, strict=False))
    assert [(d.unit_id, d.case_id, d.decision) for d in decisions] == [(70, 22, EXCLUDED), (70, 30, EXCLUDED)]


def test_a_somatic_case_level_reason_drops_that_case_only():
    rows = [*calling_task_rows(task_id=70, case_id=22), *calling_task_rows(task_id=70, case_id=30)]
    rows[1]["exclusion_reason"] = "tenant_excluded"
    decisions = somatic_decisions(rows, select_somatic(rows, strict=False))
    assert [(d.unit_id, d.case_id, d.decision) for d in decisions] == [(70, 22, RUN), (70, 30, SKIPPED)]


def test_a_somatic_experiment_without_aliquot_is_named_by_its_experiment():
    rows = calling_task_rows(task_id=70, case_id=22, aliquot=None)
    decisions = somatic_decisions(rows, select_somatic(rows, strict=False))
    assert [(d.scope, d.seq_id, d.reason) for d in decisions] == [
        ("case", None, "missing_aliquot"),
        ("member", 64, "missing_aliquot"),
    ]


def test_the_import_runs_each_case_once_with_its_first_reason():
    rows = [
        {"case_id": 4, "tenant_code": "radiant", "reason": "not_loaded"},
        {"case_id": 4, "tenant_code": "radiant", "reason": "new_or_changed"},
        {"case_id": 5, "tenant_code": None, "reason": "deleted"},
    ]
    assert [(d.case_id, d.decision, d.reason) for d in import_decisions(rows)] == [
        (4, RUN, "new_or_changed"),
        (5, RUN, "deleted"),
    ]


def test_the_cases_with_work_are_the_run_decisions_of_any_pipeline():
    decisions = decide(
        trio(),
        [],
        calling_task_rows(case_id=5),
        [qc_member_row(case_id=3, exclusion_reason="pending_alignment")],
        [{"case_id": 4, "tenant_code": "radiant", "reason": "deleted"}],
    )
    assert run_case_ids(decisions) == {1072, 5, 4}
    assert run_units(decisions, SOMATIC_SNV) == {(70, 5)}
    assert run_units(decisions, GERMLINE_SNV) == {(None, 1072)}


def test_a_child_keeps_only_the_decided_units_and_waits_the_others():
    """Germline: the unit is the case. A case runnable now but not decided waits for the next run."""
    rows = [*trio(), *[{**r, "case_id": 2000} for r in trio()]]
    selection = select_snv(rows, strict=False)
    kept, decisions = keep_decided(snv(rows), {(None, 1072)})
    assert kept == {(None, 1072)}
    assert [(d.case_id, d.decision, d.reason) for d in decisions] == [(1072, RUN, None), (2000, WAIT, NOT_DECIDED)]
    narrowed = narrow(selection, kept)
    assert narrowed.case_ids == [1072]
    assert {m.case_id for m in narrowed.members} == {1072}


def test_a_somatic_child_keeps_the_decided_task_not_the_whole_case():
    """A task that reached a decided case after the decision does not ride on it."""
    rows = [*calling_task_rows(task_id=70, case_id=22), *calling_task_rows(task_id=72, case_id=22)]
    selection = select_somatic(rows, strict=False)
    kept, decisions = keep_decided(somatic_decisions(rows, selection), {(70, 22)})
    assert [(d.unit_id, d.decision, d.reason) for d in decisions] == [(70, RUN, None), (72, WAIT, NOT_DECIDED)]
    assert [(t.task_id, [c.case_id for c in t.cases]) for t in narrow(selection, kept).tasks] == [(70, [22])]


def _launch_context(conf):
    return {"dag_run": SimpleNamespace(conf=conf), "run_id": "scheduled-2026-11-12T00-00-00-00-00"}


def _record(monkeypatch, decided=None):
    written = []
    if decided is None:
        monkeypatch.setattr(decision, "read_run_units", lambda *_: pytest.fail("no scheduler run to follow"))
    else:
        monkeypatch.setattr(decision, "read_run_units", lambda run_id, pipeline, conf=None: decided)
    monkeypatch.setattr(
        decision,
        "write_decisions",
        lambda decisions, run_id, stage, conf=None: written.append((run_id, stage, summary(decisions))),
    )
    return written


def test_launch_follows_the_scheduler_run_and_logs_only_where_it_differs(monkeypatch):
    """Decided: 1072 and 3000. Now: 1072 runs as decided (no row), 2000 became runnable since
    (`not_decided`), 3000 lost its father's alignment (`wait`, with its member row)."""
    written = _record(monkeypatch, decided={(None, 1072), (None, 3000)})
    rows = [
        *trio(),
        *[{**r, "case_id": 2000} for r in trio()],
        *[{**r, "case_id": 3000} for r in trio(father={"exclusion_reason": "pending_alignment"})],
    ]
    selection = decision.launch(
        GERMLINE_SNV, rows, select_snv(rows, strict=False), _launch_context({"decision_run_id": "scheduled-x"})
    )
    assert selection.case_ids == [1072]
    assert written == [
        (
            "scheduled-2026-11-12T00-00-00-00-00",
            "launch",
            [
                ("case", 2000, None, None, None, WAIT, NOT_DECIDED),
                ("case", 3000, None, None, None, WAIT, "pending_alignment"),
                ("member", 3000, 101, 501, 901, WAIT, "pending_alignment"),
            ],
        )
    ]


def test_launch_exactly_as_decided_logs_nothing(monkeypatch):
    written = _record(monkeypatch, decided={(None, 1072)})
    rows = trio()
    decision.launch(GERMLINE_SNV, rows, select_snv(rows, strict=False), _launch_context({"decision_run_id": "x"}))
    assert written == [("scheduled-2026-11-12T00-00-00-00-00", "launch", [])]


def test_launch_by_hand_runs_the_whole_selection_and_logs_every_decision(monkeypatch):
    written = _record(monkeypatch)
    rows = [*trio(), *[{**r, "case_id": 2000} for r in trio()]]
    selection = decision.launch(GERMLINE_SNV, rows, select_snv(rows, strict=False), _launch_context({}))
    assert selection.case_ids == [1072, 2000]
    assert [row[1] for row in written[0][2]] == [1072, 2000]
