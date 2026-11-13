"""Turn the somatic discovery result set into validated `CallingTask` objects.

Decided from the fact rows, with the same strict / lenient split as
`radiant.tasks.nextflow.germline_snv.resolve`: an operator who named calling tasks gets an error for
any that cannot run; a discovered scope drops them with a reason and runs the rest. What differs
is the unit, a calling task rather than a case, and so the granularity of an exclusion:

- a reason about the task (its experiments, its VCF, its aliquots) excludes the task from every
  case it belongs to: the pipeline runs once per task;
- a reason about one case of it (no project code, tenant excluded) drops that case only. The task
  still runs for its other cases, if it has any left.
"""

import logging
from collections import defaultdict

from pydantic import BaseModel

from radiant.tasks.nextflow.problem import Problem, case_problem
from radiant.tasks.nextflow.selection import CASE_REASON_TEXT, SEQUENCING_TYPES, CaseResolutionError
from radiant.tasks.nextflow.somatic_snv.model import NORMAL, TUMORAL, CallingTask, CallingTaskRow, SomaticCase

LOGGER = logging.getLogger(__name__)

__all__ = ["CaseResolutionError", "ExcludedTask", "Selection", "select_tasks", "tenants_of"]

# The portal accepts one aliquot on this type (`SingleAliquotTaskTypes`), so its annotation is
# always tumor-only.
SINGLE_ALIQUOT_TASK_TYPE = "alignment_somatic_variant_calling"

TASK_REASON_TEXT = {
    "pending_sequencing": "one of its sequencing experiments is not `completed` yet",
    "no_ssnv": (
        "it published no somatic SNV VCF (`ssnv`/`vcf`) -- either the VCF is typed otherwise at the "
        "source, or the task was sent the old way, with its annotation from QLIN"
    ),
    "ambiguous_ssnv": (
        "it published more than one somatic SNV VCF. One calling task cannot legitimately produce "
        "two, so a document is mistyped at the source -- typically an index recorded with format_code "
        "'vcf' instead of 'tbi'"
    ),
}


class ExcludedTask(BaseModel):
    """A calling task that will not be run for `case_ids`, and why. `reason` is the first problem
    found; `problems` is every one, for the decision log."""

    task_id: int
    case_ids: list[int]
    reason: str
    detail: str
    problems: list[Problem] = []


class Selection(BaseModel):
    """What `select_tasks` decided: what runs, what does not. `done`: the (calling task, case) pairs
    whose VCF is already annotated on the case. `untriggered`: the pairs whose task published no
    VCF, and the cases no calling task belongs to (task None). Both stay empty on a targeted run."""

    tasks: list[CallingTask] = []
    excluded: list[ExcludedTask] = []
    done: list[tuple[int, int]] = []
    untriggered: list[tuple[int | None, int]] = []

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
    excluded_tenants: list[str] | None = None,
) -> Selection:
    """Decide every (calling task, case) pair of the fact rows: run, done, untriggered, or excluded
    with its reasons. The work is a task's VCF not annotated on the case yet.

    `strict` defaults to "an operator asked for specific tasks", i.e. to whether
    `requested_task_ids` is non-empty. A targeted run only looks at the requested tasks.
    """
    requested = set(requested_task_ids or [])
    if strict is None:
        strict = bool(requested)
    blocked = set(excluded_tenants or [])

    selection = Selection()
    by_task: dict[int, dict[int, list[CallingTaskRow]]] = defaultdict(lambda: defaultdict(list))
    for row in (CallingTaskRow(**r) for r in rows):
        if row.calling_task_id is None:
            if not requested:
                selection.untriggered.append((None, row.case_id))
            continue
        by_task[row.calling_task_id][row.case_id].append(row)

    candidates: dict[int, dict[int, list[CallingTaskRow]]] = defaultdict(dict)
    for task_id in sorted(by_task):
        for case_id, case_rows in sorted(by_task[task_id].items()):
            first = case_rows[0]
            if requested:
                if task_id in requested and not first.annotated:
                    candidates[task_id][case_id] = case_rows
            elif first.annotated:
                selection.done.append((task_id, case_id))
            elif first.ssnv_matches == 0:
                selection.untriggered.append((task_id, case_id))
            else:
                candidates[task_id][case_id] = case_rows

    problems: list[str] = []
    if requested:
        unmatched = sorted(requested - set(candidates))
        if unmatched:
            problems.append(
                f"calling task(s) {unmatched} produced no candidate -- either the id does not exist or "
                f"is not a somatic calling task, no somatic case holds all of its sequencing experiments, "
                f"or its `ssnv` VCF is already the input of a `radiant_somatic_annotation`"
            )

    for task_id, rows_by_case in candidates.items():
        task, excluded = _resolve(task_id, rows_by_case, blocked)
        selection.excluded += excluded
        problems += [e.detail for e in excluded]
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
            excluded.detail,
        )
    if problems and not strict:
        LOGGER.warning(
            "%d calling task(s) or case(s) excluded, %d task(s) kept", len(selection.excluded), len(selection.tasks)
        )
    return selection


def tenants_of(tasks: list[CallingTask]) -> list[str]:
    """One batch PATCH per tenant; this is the only tenant-scoped step of the DAG."""
    return sorted({tenant for task in tasks for tenant in task.tenants})


def _resolve(
    task_id: int, rows_by_case: dict[int, list[CallingTaskRow]], excluded_tenants: set[str]
) -> tuple[CallingTask | None, list[ExcludedTask]]:
    case_ids = sorted(rows_by_case)
    # The experiments of a task do not depend on the case framing it, so any case's rows describe it.
    rows = rows_by_case[case_ids[0]]

    task_problems = _validate_task(task_id, rows)
    if task_problems:
        detail = "\n".join(problem.detail for problem in task_problems)
        excluded = ExcludedTask(
            task_id=task_id, case_ids=case_ids, reason=task_problems[0].reason, detail=detail, problems=task_problems
        )
        return None, [excluded]

    cases, excluded = [], []
    for case_id in case_ids:
        case_rows = rows_by_case[case_id]
        reason = _case_reason(case_rows[0], excluded_tenants)
        if reason:
            text = CASE_REASON_TEXT[reason]
            detail = f"calling task {task_id}, case {case_id}: {text}"
            excluded.append(
                ExcludedTask(
                    task_id=task_id,
                    case_ids=[case_id],
                    reason=reason,
                    detail=detail,
                    problems=[case_problem(reason, detail)],
                )
            )
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


def _task_reasons(rows: list[CallingTaskRow]) -> list[str]:
    """The reasons about the task's experiments and VCF. Pending sequencing is transient: the task waits."""
    reasons = []
    if any(r.seq_status != "completed" for r in rows):
        reasons.append("pending_sequencing")
    matches = rows[0].ssnv_matches
    if matches == 0:
        reasons.append("no_ssnv")
    elif matches > 1:
        reasons.append("ambiguous_ssnv")
    return reasons


def _case_reason(row: CallingTaskRow, excluded_tenants: set[str]) -> str | None:
    """The reason about one case of the task, read off any of its rows."""
    if row.project_code is None:
        return "no_project_code"
    if row.tenant_code in excluded_tenants:
        return "tenant_excluded"
    return None


def _validate_task(task_id: int, rows: list[CallingTaskRow]) -> list[Problem]:
    """`(reason, detail)` pairs about the task itself, whichever case frames it."""
    problems: list[Problem] = []

    for reason in _task_reasons(rows):
        problems.append(case_problem(reason, f"calling task {task_id}: {TASK_REASON_TEXT[reason]}"))
    if problems:
        return problems

    for row in rows:
        if not row.aliquot:
            problems.append(
                Problem(
                    reason="missing_aliquot",
                    detail=f"calling task {task_id}: sequencing experiment {row.seq_id} has no aliquot",
                    seq_id=row.seq_id,
                )
            )

    # The annotation task copies these aliquots, and the portal and the import classify it TO or
    # TN from their histology. Anything but one tumor, plus at most one normal, would be
    # classified wrongly, or refused.
    histologies = sorted(str(r.histology) for r in rows)
    if histologies not in ([TUMORAL], sorted([NORMAL, TUMORAL])):
        problems.append(
            case_problem(
                "unsupported_samples",
                f"calling task {task_id}: expected one tumoral sample, plus at most one normal one, "
                f"found {histologies}",
            )
        )
    elif rows[0].calling_task_type == SINGLE_ALIQUOT_TASK_TYPE and len(rows) != 1:
        problems.append(
            case_problem(
                "unsupported_samples",
                f"calling task {task_id}: a `{SINGLE_ALIQUOT_TASK_TYPE}` has exactly one aliquot, found {len(rows)}",
            )
        )

    strategies = {r.strategy for r in rows if r.strategy}
    unknown = sorted(s for s in strategies if s.lower() not in SEQUENCING_TYPES)
    if unknown or not strategies:
        problems.append(
            case_problem(
                "unsupported_strategy", f"calling task {task_id}: unsupported experimental strategy {unknown}"
            )
        )
    elif len({SEQUENCING_TYPES[s.lower()] for s in strategies}) > 1:
        problems.append(
            case_problem(
                "unsupported_strategy",
                f"calling task {task_id}: its experiments span several strategies {sorted(strategies)}",
            )
        )

    return problems
