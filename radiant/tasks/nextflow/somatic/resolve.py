"""Turn the somatic discovery result set into validated `CallingTask` objects.

Same strict / lenient split as `radiant.tasks.nextflow.resolve`: an operator who named calling
tasks gets an error for any that cannot run; a discovered scope drops them with a reason and runs
the rest. What differs is the unit, a calling task rather than a case, and so the granularity of an
exclusion:

- a reason about the task (its experiments, its VCF, its aliquots) excludes the task from every
  case it belongs to: the pipeline runs once per task;
- a reason about one case of it (no project code, tenant excluded) drops that case only. The task
  still runs for its other cases, if it has any left.
"""

import logging
from collections import defaultdict

from pydantic import BaseModel

from radiant.tasks.nextflow.resolve import SEQUENCING_TYPES, CaseResolutionError
from radiant.tasks.nextflow.somatic.model import NORMAL, TUMORAL, CallingTask, CallingTaskRow, SomaticCase

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

CASE_REASON_TEXT = {
    "no_project_code": (
        "the case has no project_code -- `cases.project_id` did not resolve to a row in `project`, "
        "and the batch PATCH requires it to look the case up"
    ),
    "tenant_excluded": (
        "its tenant is in `excluded_tenants` (`$RADIANT_ETL_EXCLUDED_TENANTS`), which the ETL leaves "
        "out of case processing"
    ),
}


class ExcludedTask(BaseModel):
    """A calling task that will not be run for `case_ids`, and why."""

    task_id: int
    case_ids: list[int]
    reason: str
    detail: str


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

    parsed = [CallingTaskRow(**row) for row in rows]
    by_task: dict[int, dict[int, list[CallingTaskRow]]] = defaultdict(lambda: defaultdict(list))
    for row in parsed:
        by_task[row.calling_task_id][row.case_id].append(row)

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
    task_id: int, rows_by_case: dict[int, list[CallingTaskRow]]
) -> tuple[CallingTask | None, list[ExcludedTask]]:
    case_ids = sorted(rows_by_case)
    # The experiments of a task do not depend on the case framing it, so any case's rows describe it.
    rows = rows_by_case[case_ids[0]]

    task_problems = _validate_task(task_id, rows)
    if task_problems:
        reason, detail = task_problems[0]
        detail = "\n".join(d for _, d in task_problems)
        return None, [ExcludedTask(task_id=task_id, case_ids=case_ids, reason=reason, detail=detail)]

    cases, excluded = [], []
    for case_id in case_ids:
        case_rows = rows_by_case[case_id]
        reason = next((r.exclusion_reason for r in case_rows if r.exclusion_reason), None)
        if reason:
            text = CASE_REASON_TEXT.get(reason, "is not usable")
            detail = f"calling task {task_id}, case {case_id}: {text}"
            excluded.append(ExcludedTask(task_id=task_id, case_ids=[case_id], reason=reason, detail=detail))
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


def _validate_task(task_id: int, rows: list[CallingTaskRow]) -> list[tuple[str, str]]:
    """`(reason, detail)` pairs about the task itself, whichever case frames it."""
    problems: list[tuple[str, str]] = []

    for reason in dict.fromkeys(r.exclusion_reason for r in rows if r.exclusion_reason in TASK_REASON_TEXT):
        problems.append((reason, f"calling task {task_id}: {TASK_REASON_TEXT[reason]}"))
    if problems:
        return problems

    for row in rows:
        if not row.aliquot:
            problems.append(
                ("missing_aliquot", f"calling task {task_id}: sequencing experiment {row.seq_id} has no aliquot")
            )

    # The annotation task copies these aliquots, and the portal and the import classify it TO or
    # TN from their histology. Anything but one tumor, plus at most one normal, would be
    # classified wrongly, or refused.
    histologies = sorted(str(r.histology) for r in rows)
    if histologies not in ([TUMORAL], sorted([NORMAL, TUMORAL])):
        problems.append(
            (
                "unsupported_samples",
                f"calling task {task_id}: expected one tumoral sample, plus at most one normal one, "
                f"found {histologies}",
            )
        )
    elif rows[0].calling_task_type == SINGLE_ALIQUOT_TASK_TYPE and len(rows) != 1:
        problems.append(
            (
                "unsupported_samples",
                f"calling task {task_id}: a `{SINGLE_ALIQUOT_TASK_TYPE}` has exactly one aliquot, found {len(rows)}",
            )
        )

    strategies = {r.strategy for r in rows if r.strategy}
    unknown = sorted(s for s in strategies if s.lower() not in SEQUENCING_TYPES)
    if unknown or not strategies:
        problems.append(
            ("unsupported_strategy", f"calling task {task_id}: unsupported experimental strategy {unknown}")
        )
    elif len({SEQUENCING_TYPES[s.lower()] for s in strategies}) > 1:
        problems.append(
            (
                "unsupported_strategy",
                f"calling task {task_id}: its experiments span several strategies {sorted(strategies)}",
            )
        )

    return problems
