"""What each pipeline decided for each candidate case, and why: the rows of `pipeline_decision_log`.

The selections (`germline_snv/`, `germline_cnv/`, `qc/`, `somatic_snv/`) keep deciding; this module only names
their outcome. Per pipeline and candidate unit, one **case row** with the decision of the unit,
plus one **member row** per member at fault:

- `run`: nothing stops it;
- `wait`: the data will come by itself (sequencing or alignment not done yet);
- `skipped`: left out on purpose, by configuration (`excluded_tenants`);
- `excluded`: something needs fixing -- a document, the case in the portal, or an upstream
  pipeline the ETL does not support;
- `done`: nothing left to do, the outputs are registered on the case.

The unit is the case, except for somatic annotation, where it is the (calling task, case) pair:
a case can have a tumor-normal and a tumor-only calling task, and a task can serve several cases.

Airflow is imported inside the I/O functions only, like `case_status.py`: the rules stay
importable from plain unit tests.
"""

import logging
from typing import Literal

from pydantic import BaseModel

from radiant.tasks.nextflow.germline_cnv.resolve import select_cases as select_cnv_cases
from radiant.tasks.nextflow.germline_snv.resolve import select_cases as select_snv_cases
from radiant.tasks.nextflow.problem import Problem
from radiant.tasks.nextflow.qc.resolve import select_cases as select_qc_cases
from radiant.tasks.nextflow.selection import ExcludedCase
from radiant.tasks.nextflow.somatic_snv.resolve import select_tasks as select_somatic_tasks

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
DONE = "done"

DISCOVER = "discover"
AFTER = "after"

# Transient: the data will come without anyone doing anything.
WAIT_REASONS = frozenset({"pending_sequencing", "pending_alignment"})
# Deliberate: the ETL is configured to leave these out, or the case has nothing for the pipeline.
NO_TRIGGER = "no_trigger_document"
SKIP_REASONS = frozenset({"tenant_excluded", NO_TRIGGER})
# About the case (or the calling task) as a whole. The queries repeat the case-level ones on every
# member row; they are reported once, on no member.
CASE_LEVEL_REASONS = frozenset(
    {"no_project_code", "tenant_excluded", "proband_count", "unsupported_strategy", NO_TRIGGER}
)

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
    decisions += _settled(
        pipeline, [(None, c) for c in selection.done], [(None, c) for c in selection.untriggered], tenants
    )
    return sorted(decisions, key=_order)


def _settled(pipeline: str, done: list, untriggered: list, tenants: dict[int, str]) -> list[Decision]:
    """One case row per `(unit_id, case_id)` done or with no trigger document."""
    rows = [
        Decision(pipeline=pipeline, scope="case", case_id=c, unit_id=u, tenant_code=tenants.get(c), decision=DONE)
        for u, c in done
    ]
    rows += [
        Decision(
            pipeline=pipeline,
            scope="case",
            case_id=c,
            unit_id=u,
            tenant_code=tenants.get(c),
            decision=SKIPPED,
            reason=NO_TRIGGER,
        )
        for u, c in untriggered
    ]
    return rows


def somatic_decisions(rows: list[dict], selection) -> list[Decision]:
    """The decisions of the somatic selection, one unit per (calling task, case). A case no calling
    task belongs to is one unit with no task."""
    tenants = _tenants(rows)
    decisions: list[Decision] = []
    for task in selection.tasks:
        for case in task.cases:
            decisions += _unit_rows(SOMATIC_SNV, case.case_id, case.tenant_code, [], unit_id=task.task_id)
    for excluded in selection.excluded:
        problems = excluded.problems or [Problem(reason=excluded.reason, detail=excluded.detail)]
        for case_id in excluded.case_ids:
            decisions += _unit_rows(SOMATIC_SNV, case_id, tenants.get(case_id), problems, unit_id=excluded.task_id)
    decisions += _settled(SOMATIC_SNV, selection.done, selection.untriggered, tenants)
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
    excluded_tenants: list[str] | None = None,
) -> list[Decision]:
    """Every pipeline's decisions, from the rows of its discovery query. The selections are run
    lenient, as on a scheduled run: an excluded case is a decision, not an error."""
    lenient = {"strict": False, "excluded_tenants": excluded_tenants}
    decisions = [
        *germline_decisions(GERMLINE_SNV, germline_snv_rows, select_snv_cases(germline_snv_rows, **lenient)),
        *germline_decisions(GERMLINE_CNV, germline_cnv_rows, select_cnv_cases(germline_cnv_rows, **lenient)),
        *somatic_decisions(somatic_snv_rows, select_somatic_tasks(somatic_snv_rows, **lenient)),
        *germline_decisions(QC, qc_rows, select_qc_cases(qc_rows, **lenient)),
        *import_decisions(import_rows),
    ]
    for pipeline in PIPELINES:
        cases = [d for d in decisions if d.pipeline == pipeline and d.scope == "case"]
        counts = {decision: sum(d.decision == decision for d in cases) for decision in (RUN, *SEVERITY, DONE)}
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


def selected_units(selection) -> set[tuple[int | None, int]]:
    """The `(unit_id, case_id)` a selection would run: cases, or (calling task, case) for somatic."""
    if hasattr(selection, "tasks"):
        return {(task.task_id, case.case_id) for task in selection.tasks for case in task.cases}
    return {(None, case_id) for case_id in selection.case_ids}


def launch(pipeline: str, selection, context):
    """What a "from Cases" DAG launches. Triggered by the case scheduler (`decision_run_id` in its
    conf), only the units that run decided `run`: one that became runnable since waits for the next
    run. Run by hand, its whole selection. The children read the log, they do not write it."""
    conf = context["dag_run"].conf or {}
    decision_run_id = conf.get("decision_run_id")
    if not decision_run_id:
        return selection
    selected = selected_units(selection)
    decided = read_run_units(decision_run_id, pipeline, conf)
    not_decided = sorted(case_id for _, case_id in selected - decided)
    if not_decided:
        LOGGER.warning(
            "%d case(s) runnable now but not decided by %s, left for the next run: %s",
            len(not_decided),
            decision_run_id,
            not_decided,
        )
    return narrow(selection, selected & decided)


def _order(d: Decision) -> tuple:
    return (d.case_id, d.unit_id or 0, d.scope != "case", d.patient_id or 0, d.seq_id or 0)


# --- what is written --------------------------------------------------------------------------


def _signature(rows) -> frozenset:
    """What a unit's rows say, `detail` and the run aside: equal signatures are the same decision."""
    return frozenset(
        (r["scope"], r["patient_id"], r["seq_id"], r["task_id"], r["decision"], r["reason"]) for r in rows
    )


def _units(rows) -> dict[tuple, list]:
    by_unit: dict[tuple, list] = {}
    for row in rows:
        by_unit.setdefault((row["pipeline"], row["case_id"], row["unit_id"]), []).append(row)
    return by_unit


def changed(decisions: list[Decision], state_rows: list[dict]) -> list[Decision]:
    """The decisions to write: every `run` (the work of this pass, which the children and the scheduler read
    back), and any other decision of a unit whose rows differ from its last state in `case_pipeline_state`.
    A unit that keeps its decision writes nothing: the log grows with the changes, not with the cases."""
    state = {unit: _signature(rows) for unit, rows in _units(state_rows).items()}
    kept = []
    for unit, rows in _units([d.model_dump() for d in decisions]).items():
        is_run = any(r["scope"] == "case" and r["decision"] == RUN for r in rows)
        if is_run or state.get(unit) != _signature(rows):
            kept.append(unit)
    keep = set(kept)
    return [d for d in decisions if (d.pipeline, d.case_id, d.unit_id) in keep]


# --- I/O ---------------------------------------------------------------------------------------


def _starrocks_cursor():
    """A cursor on the `starrocks_conn` connection. Airflow is imported here only."""
    from airflow.hooks.base import BaseHook

    return BaseHook.get_connection("starrocks_conn").get_hook().get_conn().cursor()


def _render(sql_file: str, conf: dict | None, folder: str = "radiant", **variables) -> str:
    import jinja2

    from radiant.dags import DAGS_DIR
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    text = (DAGS_DIR / "sql" / folder / sql_file).read_text()
    return jinja2.Template(text).render(mapping=get_radiant_mapping(conf or {}), **variables)


def read_rows(folder: str, sql_file: str, conf: dict | None = None, parameters: dict | None = None, **variables):
    """The rows of `sql/<folder>/<sql_file>`, as dicts keyed by column name."""
    sql = _render(sql_file, conf, folder, **variables)
    with _starrocks_cursor() as cursor:
        cursor.execute(sql, parameters)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


# What the case scheduler reads to decide every pipeline: each "from Cases" DAG's fact queries, and
# what the import still has to load.
DISCOVERY_QUERIES = {
    GERMLINE_SNV: [("clinical", "germline_snv_facts_select.sql")],
    GERMLINE_CNV: [("clinical", "germline_cnv_facts_select.sql")],
    SOMATIC_SNV: [("clinical", "somatic_snv_facts_select.sql")],
    QC: [("clinical", "qc_germline_facts_select.sql"), ("clinical", "qc_somatic_facts_select.sql")],
    IMPORT: [("radiant", "import_pending_case_select.sql")],
}


def read_discovery(conf: dict | None = None) -> dict[str, list[dict]]:
    """The rows each pipeline decides on: its live cases (`live_only`), the settled ones keeping their state in
    `case_pipeline_state`. QC reads two queries, one per case type."""
    rows = {
        pipeline: [row for folder, sql_file in queries for row in read_rows(folder, sql_file, conf, live_only=True)]
        for pipeline, queries in DISCOVERY_QUERIES.items()
    }
    LOGGER.info("discovery rows: %s", ", ".join(f"{p} {len(r)}" for p, r in rows.items()))
    return rows


def read_facts(sql_file: str, pipeline: str, conf: dict | None = None) -> list[dict]:
    """The fact rows a "from Cases" DAG decides on. Triggered by the case scheduler
    (`decision_run_id` in its conf), only the cases that run decided `run`; run by hand, every case."""
    conf = conf or {}
    case_ids = None
    if conf.get("decision_run_id"):
        case_ids = sorted({case_id for _, case_id in read_run_units(conf["decision_run_id"], pipeline, conf)})
        if not case_ids:
            LOGGER.info("no case decided run for %s by %s", pipeline, conf["decision_run_id"])
            return []
    rows = read_rows("clinical", sql_file, conf, {"case_ids": case_ids} if case_ids else None, case_ids=case_ids)
    LOGGER.info("%d fact row(s) read for %s", len(rows), pipeline)
    return rows


def read_state(conf: dict | None = None) -> list[dict]:
    """The last state of every unit: the rows of `case_pipeline_state`."""
    with _starrocks_cursor() as cursor:
        cursor.execute(_render("case_pipeline_state_select.sql", conf))
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def write_decisions(decisions: list[Decision], run_id: str, stage: str, conf: dict | None = None) -> None:
    """Append to `pipeline_decision_log` the decisions that changed (`changed`), all with the same
    `decided_at` (UTC): one pass."""
    import datetime

    decisions = changed(decisions, read_state(conf)) if decisions else []
    if not decisions:
        LOGGER.info("no decision to write for stage %s", stage)
        return
    decided_at = datetime.datetime.now(datetime.UTC).replace(tzinfo=None, microsecond=0)
    rows = [{**d.model_dump(), "run_id": run_id, "stage": stage, "decided_at": decided_at} for d in decisions]
    sql = _render("pipeline_decision_log_insert.sql", conf)
    with _starrocks_cursor() as cursor:
        cursor.executemany(sql, rows)
    LOGGER.info("%d decision(s) written for run %s, stage %s", len(rows), run_id, stage)


def read_run_units(
    run_id: str, pipeline: str, conf: dict | None = None, stage: str = DISCOVER
) -> set[tuple[int | None, int]]:
    """The units a pipeline was decided to run by the scheduler run `run_id` at `stage`."""
    sql = _render("pipeline_decision_run_units_select.sql", conf)
    with _starrocks_cursor() as cursor:
        cursor.execute(sql, {"run_id": run_id, "pipeline": pipeline, "stage": stage})
        return {(row[0], row[1]) for row in cursor.fetchall()}


def read_run_case_ids(run_id: str, stage: str, conf: dict | None = None) -> set[int]:
    """The cases at least one pipeline was decided to run by the scheduler run `run_id` at `stage`:
    the work waiting, as the log has it."""
    return {case_id for pipeline in PIPELINES for _, case_id in read_run_units(run_id, pipeline, conf, stage)}
