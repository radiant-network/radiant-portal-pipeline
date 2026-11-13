"""The decision log rows: what each selection decided, per unit and per member at fault."""

from types import SimpleNamespace

import pytest

from radiant.tasks.nextflow import decision
from radiant.tasks.nextflow.decision import (
    AFTER,
    DISCOVER,
    DONE,
    EXCLUDED,
    GERMLINE_SNV,
    RUN,
    SKIPPED,
    SOMATIC_SNV,
    WAIT,
    case_decision,
    decide,
    decision_of,
    germline_decisions,
    import_decisions,
    run_case_ids,
    run_units,
    somatic_decisions,
    to_log,
)
from radiant.tasks.nextflow.germline_cnv.resolve import REASON_TEXT as CNV_REASONS
from radiant.tasks.nextflow.germline_snv.resolve import REASON_TEXT as SNV_REASONS
from radiant.tasks.nextflow.germline_snv.resolve import select_cases as select_snv
from radiant.tasks.nextflow.problem import Problem
from radiant.tasks.nextflow.qc.resolve import REASON_TEXT as QC_REASONS
from radiant.tasks.nextflow.selection import CASE_REASON_TEXT
from radiant.tasks.nextflow.somatic_snv.resolve import TASK_REASON_TEXT
from radiant.tasks.nextflow.somatic_snv.resolve import select_tasks as select_somatic
from tests.unit.nextflow.germline_cnv.conftest import member_row as cnv_member_row
from tests.unit.nextflow.germline_snv.conftest import member_row as snv_member_row
from tests.unit.nextflow.qc.conftest import member_row as qc_member_row
from tests.unit.nextflow.somatic_snv.conftest import calling_task_rows

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
    `by_role` overrides a member's facts, e.g. `father={"gvcf_matches": 2}`."""
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


def snv(rows: list[dict], excluded_tenants=None) -> list[decision.Decision]:
    return germline_decisions(GERMLINE_SNV, rows, select_snv(rows, strict=False, excluded_tenants=excluded_tenants))


def summary(decisions) -> list[tuple]:
    return [(d.scope, d.case_id, d.patient_id, d.seq_id, d.task_id, d.decision, d.reason) for d in decisions]


@pytest.mark.parametrize("reason", sorted(EVERY_REASON))
def test_every_reason_code_maps_to_one_decision(reason):
    expected = {
        "pending_sequencing": WAIT,
        "pending_alignment": WAIT,
        "tenant_excluded": SKIPPED,
        "no_trigger_document": SKIPPED,
    }.get(reason, EXCLUDED)
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
        father={"alignment_task_id": None, "gvcf_url": None, "gvcf_matches": 0},
        mother={"gvcf_matches": 2},
    )
    assert summary(snv(rows)) == [
        ("case", 1072, None, None, None, EXCLUDED, "ambiguous_gvcf"),
        ("member", 1072, 101, 501, None, WAIT, "pending_alignment"),
        ("member", 1072, 102, 502, 902, EXCLUDED, "ambiguous_gvcf"),
    ]


def test_a_member_never_sequenced_has_no_experiment():
    rows = trio(mother={"seq_id": None, "alignment_task_id": None, "gvcf_url": None, "gvcf_matches": 0})
    assert summary(snv(rows))[1] == ("member", 1072, 102, None, None, WAIT, "pending_sequencing")


@pytest.mark.parametrize("reason", ["no_project_code", "tenant_excluded"])
def test_a_case_level_reason_gives_no_member_row(reason):
    if reason == "no_project_code":
        decisions = snv(trio(**{role: {"project_code": None} for role in ("proband", "father", "mother")}))
    else:
        decisions = snv(trio(), excluded_tenants=["radiant"])
    assert summary(decisions) == [("case", 1072, None, None, None, decision_of(reason), reason)]


def test_a_python_reason_about_the_case_gives_no_member_row():
    rows = trio(father={"role": "proband"})
    assert summary(snv(rows)) == [("case", 1072, None, None, None, EXCLUDED, "proband_count")]


def test_a_member_without_aliquot_is_named():
    rows = trio(mother={"aliquot": None})
    assert summary(snv(rows))[1] == ("member", 1072, 102, 502, 902, EXCLUDED, "missing_aliquot")


def test_the_tenant_comes_from_the_rows():
    rows = trio(proband={"gvcf_url": None, "gvcf_matches": 0})
    assert {d.tenant_code for d in snv(rows)} == {"radiant"}


def test_qc_decides_germline_and_somatic_cases_as_one_pipeline():
    rows = [qc_member_row(case_id=3), qc_member_row(case_id=23, case_type="somatic", affected_status="unknown")]
    decisions = decide([], [], [], rows, [])
    assert [(d.pipeline, d.case_id, d.decision) for d in decisions] == [("qc", 3, RUN), ("qc", 23, RUN)]


def test_somatic_units_are_calling_task_and_case_pairs():
    """A case with a tumor-normal call that runs and a tumor-only call without VCF: two units."""
    rows = [*calling_task_rows(task_id=70, case_id=22, normal=True), *calling_task_rows(task_id=71, case_id=22)]
    rows[-1].update(ssnv_url=None, ssnv_matches=0)
    decisions = somatic_decisions(rows, select_somatic(rows, strict=False))
    assert [(d.scope, d.unit_id, d.case_id, d.decision, d.reason) for d in decisions] == [
        ("case", 70, 22, RUN, None),
        ("case", 71, 22, SKIPPED, "no_trigger_document"),
    ]


def test_a_somatic_task_excluded_for_two_cases_gives_two_case_rows():
    rows = [*calling_task_rows(task_id=70, case_id=22), *calling_task_rows(task_id=70, case_id=30)]
    for row in rows:
        row["ssnv_matches"] = 2
    decisions = somatic_decisions(rows, select_somatic(rows, strict=False))
    assert [(d.unit_id, d.case_id, d.decision) for d in decisions] == [(70, 22, EXCLUDED), (70, 30, EXCLUDED)]


def test_a_somatic_case_level_reason_drops_that_case_only():
    rows = [*calling_task_rows(task_id=70, case_id=22), *calling_task_rows(task_id=70, case_id=30)]
    rows[1]["tenant_code"] = "other"
    decisions = somatic_decisions(rows, select_somatic(rows, strict=False, excluded_tenants=["other"]))
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
        [qc_member_row(case_id=3, alignment_task_id=None, documents=False), qc_member_row(case_id=3, patient_id=101)],
        [{"case_id": 4, "tenant_code": "radiant", "reason": "deleted"}],
    )
    assert run_case_ids(decisions) == {1072, 5, 4}
    assert run_units(decisions, SOMATIC_SNV) == {(70, 5)}
    assert run_units(decisions, GERMLINE_SNV) == {(None, 1072)}


def _launch(monkeypatch, selection, decided, conf=None):
    monkeypatch.setattr(decision, "read_run_units", lambda run_id, pipeline, conf=None: decided)
    monkeypatch.setattr(decision, "write_decisions", lambda *_a, **_k: pytest.fail("a child does not write the log"))
    context = {"dag_run": SimpleNamespace(conf={"decision_run_id": "scheduled-x"} if conf is None else conf)}
    pipeline = SOMATIC_SNV if hasattr(selection, "tasks") else GERMLINE_SNV
    return decision.launch(pipeline, selection, context)


def test_a_child_launches_only_the_decided_cases(monkeypatch, caplog):
    """Germline: the unit is the case. A case runnable now but not decided waits for the next run."""
    rows = [*trio(), *[{**r, "case_id": 2000} for r in trio()]]
    narrowed = _launch(monkeypatch, select_snv(rows, strict=False), {(None, 1072)})
    assert narrowed.case_ids == [1072]
    assert {m.case_id for m in narrowed.members} == {1072}
    assert "not decided by scheduled-x, left for the next run: [2000]" in caplog.text


def test_a_somatic_child_launches_the_decided_task_not_the_whole_case(monkeypatch):
    """A task that reached a decided case after the decision does not ride on it."""
    rows = [*calling_task_rows(task_id=70, case_id=22), *calling_task_rows(task_id=72, case_id=22)]
    narrowed = _launch(monkeypatch, select_somatic(rows, strict=False), {(70, 22)})
    assert [(t.task_id, [c.case_id for c in t.cases]) for t in narrowed.tasks] == [(70, [22])]


def test_a_child_run_by_hand_launches_its_whole_selection(monkeypatch):
    monkeypatch.setattr(decision, "read_run_units", lambda *_: pytest.fail("no scheduler run to follow"))
    rows = [*trio(), *[{**r, "case_id": 2000} for r in trio()]]
    selection = decision.launch(GERMLINE_SNV, select_snv(rows, strict=False), {"dag_run": SimpleNamespace(conf={})})
    assert selection.case_ids == [1072, 2000]


# --- done, no trigger document, and what is written --------------------------------------


def test_a_done_case_is_one_done_row():
    rows = trio(**{role: {"annotated": True} for role in ("proband", "father", "mother")})
    assert summary(snv(rows)) == [("case", 1072, None, None, None, DONE, None)]


def test_a_case_without_a_trigger_document_is_skipped_once():
    rows = trio(**{role: {"gvcf_url": None, "gvcf_matches": 0} for role in ("proband", "father", "mother")})
    assert summary(snv(rows)) == [("case", 1072, None, None, None, SKIPPED, "no_trigger_document")]


def _case(case_id, outcome, reason=None, pipeline=GERMLINE_SNV):
    return decision.Decision(pipeline=pipeline, scope="case", case_id=case_id, decision=outcome, reason=reason)


def test_done_is_never_written_at_discover():
    decisions = [_case(1, RUN), _case(2, DONE)]
    assert [d.case_id for d in to_log(decisions, DISCOVER, {}, set())] == [1]


def test_done_is_written_after_for_the_units_this_run_started_only():
    decisions = [_case(2, DONE), _case(3, DONE)]
    kept = to_log(decisions, AFTER, {GERMLINE_SNV: {(None, 2)}}, set())
    assert [d.case_id for d in kept] == [2]


def test_no_trigger_document_is_written_for_submitted_cases_only():
    decisions = [_case(4, SKIPPED, "no_trigger_document"), _case(5, SKIPPED, "no_trigger_document")]
    assert [d.case_id for d in to_log(decisions, DISCOVER, {}, {5})] == [5]


def test_the_other_decisions_are_always_written():
    decisions = [_case(1, RUN), _case(6, WAIT, "pending_alignment"), _case(7, SKIPPED, "tenant_excluded")]
    assert to_log(decisions, AFTER, {}, set()) == decisions


# --- the facts a child reads ---------------------------------------------------------------


class _Cursor:
    """A StarRocks cursor that records the statement and returns no row."""

    def __init__(self, executed):
        self.executed = executed
        self.description = [("case_id",)]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, args=None):
        self.executed.append((sql, args))

    def fetchall(self):
        return []


def _starrocks(monkeypatch) -> list:
    from types import SimpleNamespace

    executed = []
    conn = SimpleNamespace(cursor=lambda: _Cursor(executed))
    hook = SimpleNamespace(get_conn=lambda: conn)
    monkeypatch.setattr("airflow.hooks.base.BaseHook.get_connection", lambda _: SimpleNamespace(get_hook=lambda: hook))
    return executed


def test_a_triggered_child_reads_the_facts_of_the_decided_cases_only(monkeypatch):
    executed = _starrocks(monkeypatch)
    monkeypatch.setattr(decision, "read_run_units", lambda run_id, pipeline, conf=None: {(None, 9), (None, 3)})
    decision.read_facts("germline_snv_facts_select.sql", GERMLINE_SNV, {"decision_run_id": "run-1"})
    ((sql, args),) = executed
    assert "AND pc.case_id IN %(case_ids)s" in sql
    assert args == {"case_ids": [3, 9]}


def test_a_triggered_child_with_nothing_decided_reads_nothing(monkeypatch):
    executed = _starrocks(monkeypatch)
    monkeypatch.setattr(decision, "read_run_units", lambda run_id, pipeline, conf=None: set())
    assert decision.read_facts("germline_snv_facts_select.sql", GERMLINE_SNV, {"decision_run_id": "run-1"}) == []
    assert executed == []


def test_a_child_run_by_hand_reads_every_case(monkeypatch):
    executed = _starrocks(monkeypatch)
    monkeypatch.setattr(decision, "read_run_units", lambda *_: pytest.fail("no scheduler run to follow"))
    decision.read_facts("germline_snv_facts_select.sql", GERMLINE_SNV, {})
    ((sql, args),) = executed
    assert "%(case_ids)s" not in sql
    assert args is None


def test_every_pipeline_logs_its_done_and_untriggered_units():
    decisions = decide(
        [],
        [cnv_member_row(case_id=2, annotated=True)],
        calling_task_rows(case_id=5, ssnv_url=None, ssnv_matches=0),
        [qc_member_row(case_id=3, documents=False)],
        [],
    )
    assert [(d.pipeline, d.unit_id, d.case_id, d.decision, d.reason) for d in decisions] == [
        ("germline_cnv", None, 2, DONE, None),
        ("somatic_snv", 70, 5, SKIPPED, "no_trigger_document"),
        ("qc", None, 3, SKIPPED, "no_trigger_document"),
    ]
