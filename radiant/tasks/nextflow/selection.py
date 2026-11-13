"""What the case selections of every pipeline share: the member order, the sequencing types, the
strict-mode error, the excluded case, and the decision of a case-based pipeline from its facts."""

import logging
from collections.abc import Callable

from pydantic import BaseModel

from radiant.tasks.nextflow.problem import Problem, case_problem

LOGGER = logging.getLogger(__name__)

# Order members are emitted in: proband first, then parents, then everyone else.
REL_ORDER = {"proband": 0, "father": 1, "mother": 2, "brother": 3, "sister": 3, "sibling": 3, "fetus": 4}

# The pipeline's `sequencingType` column, uppercase. `wxs` and `wes` are the same thing
# under two dictionaries; the pipeline only knows WES.
SEQUENCING_TYPES = {"wgs": "WGS", "wxs": "WES", "wes": "WES"}


class CaseResolutionError(Exception):
    """One or more requested cases cannot be turned into a valid pipeline input."""


class ExcludedCase(BaseModel):
    """A candidate case that will not be run, and why. `reason` and `detail` are the first
    problem found, as the warnings and the strict-mode error report it; `problems` is every one,
    with the member it is about, for the decision log."""

    case_id: int
    reason: str
    detail: str
    problems: list[Problem] = []


# Why a case cannot run, about the case as a whole. Shared wording for every pipeline.
CASE_REASON_TEXT = {
    "no_project_code": (
        "the case has no project_code -- `cases.project_id` did not resolve to a row in "
        "`project`, and the batch PATCH requires it to look the case up"
    ),
    "tenant_excluded": (
        "its tenant is in `excluded_tenants` (`$RADIANT_ETL_EXCLUDED_TENANTS`), which the ETL "
        "leaves out of case processing"
    ),
}


class CaseDecisions(BaseModel):
    """What `decide_cases` decided for a case-based pipeline. `done`: annotated, and no member's
    trigger document is left to process. `untriggered`: never processed, and no member has a
    trigger document. Both stay empty on a targeted run, which only looks at the requested tasks."""

    members: list = []
    case_ids: list[int] = []
    excluded: list[ExcludedCase] = []
    done: list[int] = []
    untriggered: list[int] = []


def document_reason(member, matches: int, missing: str, ambiguous: str) -> str | None:
    """Why a member keeps its case from running, most specific first: a member with no experiment
    has no alignment either. The two `pending_*` are transient: the case waits."""
    if member.seq_id is None:
        return "pending_sequencing"
    if member.alignment_task_id is None:
        return "pending_alignment"
    if matches == 0:
        return missing
    if matches > 1:
        return ambiguous
    return None


def case_problems(case_id: int, case, excluded_tenants: set[str]) -> list[Problem]:
    """The reasons about the case as a whole, read off any of its rows."""
    problems = []
    if case.project_code is None:
        problems.append(case_problem("no_project_code", f"case {case_id}: {CASE_REASON_TEXT['no_project_code']}"))
    if case.tenant_code in excluded_tenants:
        problems.append(case_problem("tenant_excluded", f"case {case_id}: {CASE_REASON_TEXT['tenant_excluded']}"))
    return problems


def group_by_case(members: list) -> dict[int, list]:
    """The members of each case, proband first, then parents, then everyone else."""
    by_case: dict[int, list] = {}
    for member in members:
        by_case.setdefault(member.case_id, []).append(member)
    for case_members in by_case.values():
        case_members.sort(key=lambda m: (REL_ORDER.get(m.role, 9), m.patient_id))
    return by_case


def decide_cases(
    members: list,
    *,
    triggered: Callable,
    validate: Callable,
    requested_task_ids: list[int] | None,
    strict: bool | None,
    unmatched_text: str,
) -> CaseDecisions:
    """Decide every case of a case-based pipeline (germline SNV, germline CNV, QC) from its fact rows.

    - **work**: a member has its trigger document (`triggered`) and is not `annotated` on the case.
      The case is a candidate, and `validate` lists its problems; a case with any is excluded.
    - **done**: otherwise, annotated for at least one member.
    - **untriggered**: otherwise.

    `strict` defaults to "an operator asked for specific tasks". A targeted run only looks at the
    cases a requested alignment task still has work for, and raises on any problem.
    """
    requested = set(requested_task_ids or [])
    if strict is None:
        strict = bool(requested)

    decisions = CaseDecisions()
    candidates: dict[int, list] = {}
    for case_id, case_members in sorted(group_by_case(members).items()):
        pending = [m for m in case_members if triggered(m) and not m.annotated]
        if requested:
            if any(m.alignment_task_id in requested for m in pending):
                candidates[case_id] = case_members
        elif pending:
            candidates[case_id] = case_members
        elif any(m.annotated for m in case_members):
            decisions.done.append(case_id)
        else:
            decisions.untriggered.append(case_id)

    problems: list[str] = []
    if requested:
        found = {m.alignment_task_id for ms in candidates.values() for m in ms if m.alignment_task_id is not None}
        unmatched = sorted(requested - found)
        if unmatched:
            problems.append(f"alignment task(s) {unmatched} produced no candidate case -- {unmatched_text}")

    for case_id, case_members in candidates.items():
        found_problems = validate(case_id, case_members)
        if found_problems:
            first = found_problems[0]
            decisions.excluded.append(
                ExcludedCase(case_id=case_id, reason=first.reason, detail=first.detail, problems=found_problems)
            )
            problems += [problem.detail for problem in found_problems]
            continue
        decisions.members += case_members
        decisions.case_ids.append(case_id)

    if problems and strict:
        raise CaseResolutionError("\n".join(problems))

    for excluded in decisions.excluded:
        LOGGER.warning("case %d excluded (%s): %s", excluded.case_id, excluded.reason, excluded.detail)
    if problems and not strict:
        LOGGER.warning("%d candidate case(s) excluded, %d kept", len(decisions.excluded), len(decisions.case_ids))
    return decisions
