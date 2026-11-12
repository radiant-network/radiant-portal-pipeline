"""What each pipeline decided for each candidate case, and why: the rows of `pipeline_decision_log`.

The selections (`resolve.py`, `cnv/`, `qc/`, `somatic/`) keep deciding; this module only names
their outcome. Per pipeline and candidate unit, one **case row** with the decision of the unit,
plus one **member row** per member at fault:

- `run`: nothing stops it;
- `wait`: the data will come by itself (sequencing or alignment not done yet);
- `skipped`: left out on purpose, by configuration (`excluded_tenants`);
- `excluded`: something needs fixing -- a document, the case in the portal, or an upstream
  pipeline the ETL does not support.

The unit is the case, except for somatic annotation, where it is the (calling task, case) pair:
a case can have a tumor-normal and a tumor-only calling task, and a task can serve several cases.

Airflow is imported inside the I/O functions only, like `case_status.py`: the rules stay
importable from plain unit tests.
"""

import logging
from typing import Literal

from pydantic import BaseModel

from radiant.tasks.nextflow.cnv.resolve import select_cases as select_cnv_cases
from radiant.tasks.nextflow.problem import Problem
from radiant.tasks.nextflow.qc.resolve import select_cases as select_qc_cases
from radiant.tasks.nextflow.resolve import ExcludedCase
from radiant.tasks.nextflow.resolve import select_cases as select_snv_cases
from radiant.tasks.nextflow.somatic.resolve import select_tasks as select_somatic_tasks

LOGGER = logging.getLogger(__name__)

GERMLINE_SNV = "germline_snv"
GERMLINE_CNV = "germline_cnv"
SOMATIC_SNV = "somatic_snv"
QC = "qc"
IMPORT = "import"
PIPELINES = (GERMLINE_SNV, GERMLINE_CNV, SOMATIC_SNV, QC, IMPORT)

RUN = "run"
WAIT = "wait"
SKIPPED = "skipped"
EXCLUDED = "excluded"

DISCOVER = "discover"
LAUNCH = "launch"
AFTER = "after"

# Transient: the data will come without anyone doing anything.
WAIT_REASONS = frozenset({"pending_sequencing", "pending_alignment"})
# Deliberate: the ETL is configured to leave these out.
SKIP_REASONS = frozenset({"tenant_excluded"})
# About the case (or the calling task) as a whole. The queries repeat the case-level ones on every
# member row; they are reported once, on no member.
CASE_LEVEL_REASONS = frozenset({"no_project_code", "tenant_excluded", "proband_count", "unsupported_strategy"})
# A unit the child could run but that the scheduler did not decide: it is decided by the next run.
NOT_DECIDED = "not_decided"

# The most severe decision wins for the case: something to fix before a deliberate exclusion,
# before something that will come.
SEVERITY = (EXCLUDED, SKIPPED, WAIT)

# The import's reasons, the first one found wins for the case.
IMPORT_REASONS = ("new_or_changed", "not_loaded", "deleted")


class Decision(BaseModel):
    pipeline: str
    scope: Literal["case", "member"]
    case_id: int
    tenant_code: str | None = None
    patient_id: int | None = None
    seq_id: int | None = None
    task_id: int | None = None
    unit_id: int | None = None
    decision: str
    reason: str | None = None
    detail: str | None = None


def decision_of(reason: str | None) -> str:
    if not reason:
        return RUN
    if reason in WAIT_REASONS:
        return WAIT
    if reason in SKIP_REASONS:
        return SKIPPED
    return EXCLUDED


def case_decision(problems: list[Problem]) -> tuple[str, str | None, str | None]:
    """`(decision, reason, detail)` of a unit from all its problems: the most severe decision, with
    the first problem that has it (members come proband first)."""
    for decision in SEVERITY:
        first = next((p for p in problems if decision_of(p.reason) == decision), None)
        if first:
            return decision, first.reason, first.detail
    return RUN, None, None


def _member_rows(pipeline: str, case_id: int, tenant: str | None, problems: list[Problem], unit_id=None):
    rows, seen = [], set()
    for problem in problems:
        if problem.reason in CASE_LEVEL_REASONS:
            continue
        if problem.patient_id is None and problem.seq_id is None:
            continue
        key = (problem.reason, problem.patient_id, problem.seq_id)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            Decision(
                pipeline=pipeline,
                scope="member",
                case_id=case_id,
                tenant_code=tenant,
                patient_id=problem.patient_id,
                seq_id=problem.seq_id,
                task_id=problem.task_id,
                unit_id=unit_id,
                decision=decision_of(problem.reason),
                reason=problem.reason,
                detail=problem.detail,
            )
        )
    return rows


def _unit_rows(pipeline: str, case_id: int, tenant: str | None, problems: list[Problem], unit_id=None):
    decision, reason, detail = case_decision(problems)
    case_row = Decision(
        pipeline=pipeline,
        scope="case",
        case_id=case_id,
        tenant_code=tenant,
        unit_id=unit_id,
        decision=decision,
        reason=reason,
        detail=detail,
    )
    return [case_row, *_member_rows(pipeline, case_id, tenant, problems, unit_id)]


def _tenants(rows: list[dict]) -> dict[int, str]:
    return {row["case_id"]: row.get("tenant_code") for row in rows}


def _problems_of(excluded: ExcludedCase) -> list[Problem]:
    # A selection built outside `_validate` (tests, older XComs) may carry no problem list.
    return excluded.problems or [Problem(reason=excluded.reason, detail=excluded.detail)]


def germline_decisions(pipeline: str, rows: list[dict], selection) -> list[Decision]:
    """The decisions of a case-based selection (germline SNV, germline CNV, QC)."""
    tenants = _tenants(rows)
    decisions: list[Decision] = []
    for case_id in selection.case_ids:
        decisions += _unit_rows(pipeline, case_id, tenants.get(case_id), [])
    for excluded in selection.excluded:
        decisions += _unit_rows(pipeline, excluded.case_id, tenants.get(excluded.case_id), _problems_of(excluded))
    return sorted(decisions, key=_order)


def somatic_decisions(rows: list[dict], selection) -> list[Decision]:
    """The decisions of the somatic selection, one unit per (calling task, case)."""
    tenants = _tenants(rows)
    decisions: list[Decision] = []
    for task in selection.tasks:
        for case in task.cases:
            decisions += _unit_rows(SOMATIC_SNV, case.case_id, case.tenant_code, [], unit_id=task.task_id)
    for excluded in selection.excluded:
        problems = excluded.problems or [Problem(reason=excluded.reason, detail=excluded.detail)]
        for case_id in excluded.case_ids:
            decisions += _unit_rows(SOMATIC_SNV, case_id, tenants.get(case_id), problems, unit_id=excluded.task_id)
    return sorted(decisions, key=_order)


def import_decisions(rows: list[dict]) -> list[Decision]:
    """One `run` per case the import has work for, with the first reason found."""
    by_case: dict[int, dict] = {}
    for row in rows:
        current = by_case.get(row["case_id"])
        rank = IMPORT_REASONS.index(row["reason"]) if row.get("reason") in IMPORT_REASONS else len(IMPORT_REASONS)
        if current is None or rank < current["rank"]:
            by_case[row["case_id"]] = {**row, "rank": rank}
    return [
        Decision(
            pipeline=IMPORT,
            scope="case",
            case_id=case_id,
            tenant_code=row.get("tenant_code"),
            decision=RUN,
            reason=row.get("reason"),
        )
        for case_id, row in sorted(by_case.items())
    ]


def decide(
    germline_snv_rows: list[dict],
    germline_cnv_rows: list[dict],
    somatic_snv_rows: list[dict],
    qc_rows: list[dict],
    import_rows: list[dict],
) -> list[Decision]:
    """Every pipeline's decisions, from the rows of its pending query. The selections are run
    lenient, as on a scheduled run: an excluded case is a decision, not an error."""
    decisions = [
        *germline_decisions(GERMLINE_SNV, germline_snv_rows, select_snv_cases(germline_snv_rows, strict=False)),
        *germline_decisions(GERMLINE_CNV, germline_cnv_rows, select_cnv_cases(germline_cnv_rows, strict=False)),
        *somatic_decisions(somatic_snv_rows, select_somatic_tasks(somatic_snv_rows, strict=False)),
        *germline_decisions(QC, qc_rows, select_qc_cases(qc_rows, strict=False)),
        *import_decisions(import_rows),
    ]
    for pipeline in PIPELINES:
        cases = [d for d in decisions if d.pipeline == pipeline and d.scope == "case"]
        counts = {decision: sum(d.decision == decision for d in cases) for decision in (RUN, *SEVERITY)}
        LOGGER.info("%s: %s", pipeline, ", ".join(f"{n} {decision}" for decision, n in counts.items()))
    return decisions


def run_case_ids(decisions: list[Decision]) -> set[int]:
    """The cases at least one pipeline runs on: what the scheduler counts as work waiting."""
    return {d.case_id for d in decisions if d.scope == "case" and d.decision == RUN}


def run_units(decisions: list[Decision], pipeline: str) -> set[tuple[int | None, int]]:
    """The `(unit_id, case_id)` a pipeline decided to run; `unit_id` is None but for somatic."""
    return {
        (d.unit_id, d.case_id) for d in decisions if d.pipeline == pipeline and d.scope == "case" and d.decision == RUN
    }


def keep_decided(
    decisions: list[Decision], decided: set[tuple[int | None, int]]
) -> tuple[set[tuple[int | None, int]], list[Decision]]:
    """What a child launches when the scheduler decided for it: the units it would run *and* that
    were decided `run`. A unit it would run but that was not decided (its data arrived after the
    decision) becomes `wait` / `not_decided`, and the next run decides it. Returns the kept units and
    the child's decisions, rewritten accordingly."""
    kept: set[tuple[int | None, int]] = set()
    rewritten: list[Decision] = []
    for d in decisions:
        if d.scope == "case" and d.decision == RUN:
            unit = (d.unit_id, d.case_id)
            if unit in decided:
                kept.add(unit)
            else:
                d = d.model_copy(
                    update={
                        "decision": WAIT,
                        "reason": NOT_DECIDED,
                        "detail": "runnable now, but not decided by this run's case scheduler: the next run decides",
                    }
                )
        rewritten.append(d)
    return kept, rewritten


def narrow(selection, kept: set[tuple[int | None, int]]):
    """The selection restricted to the kept units: cases for a germline or QC selection, (calling
    task, case) pairs for the somatic one. The excluded list is left as is."""
    if hasattr(selection, "tasks"):
        tasks = []
        for task in selection.tasks:
            cases = [c for c in task.cases if (task.task_id, c.case_id) in kept]
            if cases:
                tasks.append(task.model_copy(update={"cases": cases}))
        return selection.model_copy(update={"tasks": tasks})
    case_ids = {case_id for _, case_id in kept}
    return selection.model_copy(
        update={
            "case_ids": [c for c in selection.case_ids if c in case_ids],
            "members": [m for m in selection.members if m.case_id in case_ids],
        }
    )


def launch(pipeline: str, rows: list[dict], selection, context) -> object:
    """What a "from Cases" DAG launches, and its decisions written at stage `launch`.

    Triggered by the case scheduler (`decision_run_id` in the run's conf), it keeps only the units
    that run decided `run`; otherwise (a run by hand) it launches its whole selection. Either way
    every decision it took is logged under its own run id.
    """
    from radiant.tasks.nextflow.paths import sanitize_run_tag

    conf = context["dag_run"].conf or {}
    decisions = (
        somatic_decisions(rows, selection)
        if pipeline == SOMATIC_SNV
        else germline_decisions(pipeline, rows, selection)
    )
    decision_run_id = conf.get("decision_run_id")
    if decision_run_id:
        kept, decisions = keep_decided(decisions, read_run_units(decision_run_id, pipeline, conf))
        not_decided = sorted({d.case_id for d in decisions if d.reason == NOT_DECIDED})
        if not_decided:
            LOGGER.warning(
                "%d case(s) runnable now but not decided by %s, left for the next run: %s",
                len(not_decided),
                decision_run_id,
                not_decided,
            )
        selection = narrow(selection, kept)
    write_decisions(decisions, sanitize_run_tag(context["run_id"]), LAUNCH, conf)
    return selection


def _order(d: Decision) -> tuple:
    return (d.case_id, d.unit_id or 0, d.scope != "case", d.patient_id or 0, d.seq_id or 0)


# --- I/O ---------------------------------------------------------------------------------------


def _render(sql_file: str, conf: dict | None) -> str:
    import jinja2

    from radiant.dags import DAGS_DIR
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    text = (DAGS_DIR / "sql" / "radiant" / sql_file).read_text()
    return jinja2.Template(text).render(mapping=get_radiant_mapping(conf or {}))


def write_decisions(decisions: list[Decision], run_id: str, stage: str, conf: dict | None = None) -> None:
    """Append the decisions to `pipeline_decision_log`, all with the same `decided_at`."""
    from airflow.hooks.base import BaseHook

    if not decisions:
        LOGGER.info("no decision to write for stage %s", stage)
        return
    rows = [{**d.model_dump(), "run_id": run_id, "stage": stage} for d in decisions]
    sql = _render("pipeline_decision_log_insert.sql", conf)
    with BaseHook.get_connection("starrocks_conn").get_hook().get_conn().cursor() as cursor:
        cursor.executemany(sql, rows)
    LOGGER.info("%d decision(s) written for run %s, stage %s", len(rows), run_id, stage)


def read_run_units(run_id: str, pipeline: str, conf: dict | None = None) -> set[tuple[int | None, int]]:
    """The units a pipeline was decided to run at `discover` by the scheduler run `run_id`."""
    from airflow.hooks.base import BaseHook

    sql = _render("pipeline_decision_run_units_select.sql", conf)
    with BaseHook.get_connection("starrocks_conn").get_hook().get_conn().cursor() as cursor:
        cursor.execute(sql, {"run_id": run_id, "pipeline": pipeline})
        return {(row[0], row[1]) for row in cursor.fetchall()}
