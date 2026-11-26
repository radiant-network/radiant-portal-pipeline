"""Turn the somatic discovery result set into validated `CallingTask` objects.

Same strict / lenient split as `radiant.tasks.nextflow.resolve`: an operator who named calling
tasks gets an error for any that cannot run; a discovered scope drops them with a reason and runs
the rest. What differs is the unit, a calling task rather than a case, and so the granularity of an
exclusion:

- a reason about the task (its experiments, its VCF, its aliquots) excludes the task from every
  case it belongs to: the pipeline runs once per task;
- a reason about one case of it (no project code, tenant excluded) drops that case only. The task
  still runs for its other cases, if it has any left.

The case scheduler decides on `somatic_snv_facts_select.sql` with `decide_tasks`: one journal item per (case,
calling task), its `item_key` the calling task.
"""

import logging
from collections import defaultdict

from pydantic import BaseModel

from radiant.tasks.decision import Decision, Reason, ReasonCode, decision_of, from_query
from radiant.tasks.journal import Entry, Pipeline
from radiant.tasks.nextflow.resolve import SEQUENCING_TYPES, CaseResolutionError
from radiant.tasks.nextflow.somatic.model import NORMAL, TUMORAL, CallingTask, CallingTaskRow, SomaticCase

LOGGER = logging.getLogger(__name__)

__all__ = [
    "CaseResolutionError",
    "ExcludedTask",
    "Selection",
    "decide_tasks",
    "outcome_entries",
    "select_run_tasks",
    "select_tasks",
    "tenants_of",
]

# The portal accepts one aliquot on this type (`SingleAliquotTaskTypes`), so its annotation is
# always tumor-only.
SINGLE_ALIQUOT_TASK_TYPE = "alignment_somatic_variant_calling"

# Reasons about the task itself: they exclude it from every case it belongs to.
TASK_REASONS = {ReasonCode.PENDING_SEQUENCING, ReasonCode.MISSING_INPUT, ReasonCode.AMBIGUOUS_INPUT}


class ExcludedTask(BaseModel):
    """A calling task that will not be run for `case_ids`, and why."""

    task_id: int
    case_ids: list[int]
    reason: Reason
    message: str


class Selection(BaseModel):
    """What `select_tasks` decided: what runs, what does not."""

    tasks: list[CallingTask] = []
    excluded: list[ExcludedTask] = []

    @property
    def case_ids(self) -> list[int]:
        return sorted({c.case_id for t in self.tasks for c in t.cases})

    @property
    def tenants(self) -> list[str]:
        return tenants_of(self.tasks)


def select_tasks(
    rows: list[dict],
    requested_task_ids: list[int] | None = None,
    *,
    strict: bool | None = None,
) -> Selection:
    """Group the candidate rows by calling task and keep the ones that can actually be run.

    `strict` defaults to "an operator asked for specific tasks", i.e. to whether
    `requested_task_ids` is non-empty.
    """
    requested = list(requested_task_ids or [])
    if strict is None:
        strict = bool(requested)

    by_task = _by_task([CallingTaskRow(**row) for row in rows])

    problems: list[str] = []
    if requested:
        unmatched = sorted(set(requested) - set(by_task))
        if unmatched:
            problems.append(
                f"calling task(s) {unmatched} produced no candidate -- either the id does not exist or "
                f"is not a somatic calling task, no somatic case holds all of its sequencing experiments, "
                f"or its `ssnv` VCF is already the input of a `radiant_somatic_annotation`"
            )

    selection = Selection()
    for task_id in sorted(by_task):
        task, excluded = _resolve(task_id, by_task[task_id])
        selection.excluded += excluded
        problems += [e.message for e in excluded]
        if task:
            selection.tasks.append(task)

    if problems and strict:
        raise CaseResolutionError("\n".join(problems))

    for excluded in selection.excluded:
        LOGGER.warning(
            "calling task %d excluded for case(s) %s (%s): %s",
            excluded.task_id,
            excluded.case_ids,
            excluded.reason,
            excluded.message,
        )
    if problems and not strict:
        LOGGER.warning(
            "%d calling task(s) or case(s) excluded, %d task(s) kept", len(selection.excluded), len(selection.tasks)
        )
    return selection


def decide_tasks(fact_rows: list[dict], excluded_tenants: list[str]) -> list[Entry]:
    """One journal entry per (case, calling task) of `somatic_snv_facts_select.sql`: the decision and all its
    reasons. A reason about the task holds for each of its cases; a reason about a case, for that case only."""
    entries = []
    for task_id, rows_by_case in sorted(_by_task(_with_reasons(fact_rows, excluded_tenants)).items()):
        case_ids = sorted(rows_by_case)
        task_reasons = [reason for reason, _ in _validate_task(task_id, rows_by_case[case_ids[0]])]
        for case_id in case_ids:
            # From the facts, not the rows' reasons: a task reason comes first there, and would hide a skip.
            first = rows_by_case[case_id][0]
            case_reasons = [
                *([Reason(code=ReasonCode.NO_PROJECT_CODE)] if first.project_code is None else []),
                *([Reason(code=ReasonCode.TENANT_EXCLUDED)] if first.tenant_code in excluded_tenants else []),
            ]
            reasons = tuple(dict.fromkeys([*task_reasons, *case_reasons]))
            # An item runs until it is annotated: every run of it is its first, or a retry after a failure.
            decision = decision_of(reasons) if reasons else Decision.RUN
            entries.append(
                Entry(
                    case_id=case_id,
                    pipeline=Pipeline.SOMATIC_SNV,
                    item_key=str(task_id),
                    decision=decision,
                    reasons=reasons or (Reason(code=ReasonCode.FIRST_RUN),),
                )
            )
    return entries


def select_run_tasks(rows: list[dict], excluded_tenants: list[str]) -> Selection:
    """The calling tasks the scheduler decided `run` for, checked again, lenient: the data may have moved since the
    decision. Only their (case, task) pairs not annotated yet come back from the query."""
    return select_tasks([r.model_dump() for r in _with_reasons(rows, excluded_tenants)], strict=False)


def outcome_entries(selection: dict, registered_tenants: set[str]) -> list[Entry]:
    """What a run of the DAG did with its (case, calling task) pairs, as journal entries: `succeeded` once the case's
    tenant batch is registered, `failed` otherwise, and `failed` with its reason for a pair left out."""
    entries = [
        Entry(
            case_id=case["case_id"],
            pipeline=Pipeline.SOMATIC_SNV,
            item_key=str(task["task_id"]),
            decision=Decision.SUCCEEDED if case["tenant_code"] in registered_tenants else Decision.FAILED,
        )
        for task in selection.get("tasks", [])
        for case in task["cases"]
    ]
    entries += [
        Entry(
            case_id=case_id,
            pipeline=Pipeline.SOMATIC_SNV,
            item_key=str(e["task_id"]),
            decision=Decision.FAILED,
            reasons=(Reason(**e["reason"]),),
        )
        for e in selection.get("excluded", [])
        for case_id in e["case_ids"]
    ]
    return entries


def _with_reasons(rows: list[dict], excluded_tenants: list[str]) -> list[CallingTaskRow]:
    parsed = [CallingTaskRow(**row) for row in rows]
    for row in parsed:
        row.exclusion_reason = _query_reason(row, excluded_tenants)
    return parsed


def _query_reason(row: CallingTaskRow, excluded_tenants: list[str]) -> str | None:
    """Most specific first, the task's reasons before the case's. `pending_sequencing` is transient: the task waits
    for its experiments rather than running on unfinished data."""
    if row.sequencing_status != "completed":
        return "pending_sequencing"
    if row.ssnv_matches == 0:
        return "no_ssnv"
    if row.ssnv_matches > 1:
        return "ambiguous_ssnv"
    if row.project_code is None:
        return "no_project_code"
    if row.tenant_code in excluded_tenants:
        return "tenant_excluded"
    return None


def _by_task(rows: list[CallingTaskRow]) -> dict[int, dict[int, list[CallingTaskRow]]]:
    by_task: dict[int, dict[int, list[CallingTaskRow]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        by_task[row.calling_task_id][row.case_id].append(row)
    return by_task


def tenants_of(tasks: list[CallingTask]) -> list[str]:
    """One batch PATCH per tenant; this is the only tenant-scoped step of the DAG."""
    return sorted({tenant for task in tasks for tenant in task.tenants})


def _resolve(
    task_id: int, rows_by_case: dict[int, list[CallingTaskRow]]
) -> tuple[CallingTask | None, list[ExcludedTask]]:
    case_ids = sorted(rows_by_case)
    # The experiments of a task do not depend on the case framing it, so any case's rows describe it.
    rows = rows_by_case[case_ids[0]]

    task_problems = _validate_task(task_id, rows)
    if task_problems:
        reason = task_problems[0][0]
        message = "\n".join(m for _, m in task_problems)
        return None, [ExcludedTask(task_id=task_id, case_ids=case_ids, reason=reason, message=message)]

    cases, excluded = [], []
    for case_id in case_ids:
        case_rows = rows_by_case[case_id]
        code = next((r.exclusion_reason for r in case_rows if r.exclusion_reason), None)
        if code:
            reason = from_query(code)
            message = f"calling task {task_id}, case {case_id}: {reason.text}"
            excluded.append(ExcludedTask(task_id=task_id, case_ids=[case_id], reason=reason, message=message))
            continue
        first = case_rows[0]
        cases.append(
            SomaticCase(
                case_id=case_id,
                submitter_case_id=first.submitter_case_id,
                tenant_code=first.tenant_code,
                project_code=first.project_code,
            )
        )
    if not cases:
        return None, excluded

    tumor = next(r for r in rows if r.histology == TUMORAL)
    normal = next((r for r in rows if r.histology == NORMAL), None)
    task = CallingTask(
        task_id=task_id,
        task_type=rows[0].calling_task_type,
        sequencing_type=SEQUENCING_TYPES[tumor.strategy.lower()],
        vcf_url=tumor.ssnv_url,
        tumor_aliquot=tumor.aliquot,
        normal_aliquot=normal.aliquot if normal else None,
        cases=cases,
    )
    return task, excluded


def _validate_task(task_id: int, rows: list[CallingTaskRow]) -> list[tuple[Reason, str]]:
    """`(reason, message)` pairs about the task itself, whichever case frames it."""
    problems: list[tuple[Reason, str]] = []

    reasons = [from_query(r.exclusion_reason) for r in rows if r.exclusion_reason]
    for reason in dict.fromkeys(r for r in reasons if r.code in TASK_REASONS):
        problems.append((reason, f"calling task {task_id}: {reason.text}"))
    if problems:
        return problems

    for row in rows:
        if not row.aliquot:
            problems.append(
                (
                    Reason(code=ReasonCode.MISSING_ALIQUOT),
                    f"calling task {task_id}: sequencing experiment {row.seq_id} has no aliquot",
                )
            )

    # The annotation task copies these aliquots, and the portal and the import classify it TO or
    # TN from their histology. Anything but one tumor, plus at most one normal, would be
    # classified wrongly, or refused.
    histologies = sorted(str(r.histology) for r in rows)
    if histologies not in ([TUMORAL], sorted([NORMAL, TUMORAL])):
        problems.append(
            (
                Reason(code=ReasonCode.UNSUPPORTED_SAMPLES),
                f"calling task {task_id}: expected one tumoral sample, plus at most one normal one, "
                f"found {histologies}",
            )
        )
    elif rows[0].calling_task_type == SINGLE_ALIQUOT_TASK_TYPE and len(rows) != 1:
        problems.append(
            (
                Reason(code=ReasonCode.UNSUPPORTED_SAMPLES),
                f"calling task {task_id}: a `{SINGLE_ALIQUOT_TASK_TYPE}` has exactly one aliquot, found {len(rows)}",
            )
        )

    strategies = {r.strategy for r in rows if r.strategy}
    unknown = sorted(s for s in strategies if s.lower() not in SEQUENCING_TYPES)
    if unknown or not strategies:
        problems.append(
            (
                Reason(code=ReasonCode.UNSUPPORTED_STRATEGY, detail=",".join(unknown)),
                f"calling task {task_id}: unsupported experimental strategy {unknown}",
            )
        )
    elif len({SEQUENCING_TYPES[s.lower()] for s in strategies}) > 1:
        problems.append(
            (
                Reason(code=ReasonCode.UNSUPPORTED_STRATEGY, detail=",".join(sorted(strategies))),
                f"calling task {task_id}: its experiments span several strategies {sorted(strategies)}",
            )
        )

    return problems
