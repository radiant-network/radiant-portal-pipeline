"""Turn the discovery result set into validated `QcCase` objects.

Same two-step shape as `radiant.tasks.nextflow.resolve`, for the same reasons: `select_cases`
decides which candidate cases can run and why the others cannot, in strict mode (an operator
named alignment tasks) or lenient mode (the nightly query found them); `resolve_cases` builds
the cases it kept. What differs is the trigger document -- a CRAM, not a gVCF; for a somatic
member, the DRAGEN metrics tarball, with no CRAM -- that a case may mix strategies, since the QC
samplesheet carries `experimentalStrategy` per row, and that a somatic case is one member, its
proband's tumor.

The case scheduler decides on `quality_control_facts_select.sql` with `decide_cases`, and on its somatic sibling with
`decide_somatic_cases`, the rule of germline SNV (`radiant.tasks.nextflow.resolve.decide`): a member absent but the
proband does not block, the family runs with the members present. The pipeline accepts it: it derives the pedigree
from the samplesheet, with `0` for a parent missing from it.
"""

import logging

from pydantic import BaseModel

from radiant.tasks.decision import Reason, ReasonCode, from_query
from radiant.tasks.journal import Entry, Pipeline
from radiant.tasks.nextflow.qc.model import SOMATIC, QcCase, QcMember, family_id
from radiant.tasks.nextflow.resolve import (
    REL_ORDER,
    CaseResolutionError,
    ExcludedCase,
    decide,
    is_absent,
    query_reason,
)

LOGGER = logging.getLogger(__name__)

# The pipeline's `experimentalStrategy` enum. `wxs` and `wes` are the same thing under two
# dictionaries; the pipeline spells it WXS.
STRATEGIES = {"wgs": "WGS", "wxs": "WXS", "wes": "WXS"}

__all__ = [
    "CaseResolutionError",
    "ExcludedCase",
    "Selection",
    "decide_cases",
    "decide_somatic_cases",
    "select_cases",
    "select_run_cases",
    "resolve_cases",
    "tenants_of",
]


class Selection(BaseModel):
    """What `select_cases` decided: what runs, what does not, and the tenants involved."""

    members: list[QcMember] = []
    case_ids: list[int] = []
    excluded: list[ExcludedCase] = []

    @property
    def tenants(self) -> list[str]:
        return sorted({m.tenant_code for m in self.members})


def select_cases(
    rows: list[dict],
    requested_task_ids: list[int] | None = None,
    *,
    strict: bool | None = None,
) -> Selection:
    """Group the candidate rows by case and keep the ones that can actually be run."""
    requested = list(requested_task_ids or [])
    if strict is None:
        strict = bool(requested)

    members = [QcMember(**row) for row in rows]
    by_case = _group_by_case(members)
    problems: list[str] = []

    if requested:
        found = {m.alignment_task_id for m in members if m.alignment_task_id is not None}
        unmatched = sorted(set(requested) - found)
        if unmatched:
            problems.append(
                f"alignment task(s) {unmatched} produced no candidate case -- either the id does "
                f"not exist, is not the current `alignment_germline_variant_calling` or "
                f"`alignment_somatic_variant_calling` task of its experiment, or every case it "
                f"belongs to already carries a `quality_control_metrics` task"
            )

    selection = Selection()
    for case_id in sorted(by_case):
        case_members = by_case[case_id]
        case_problems = _validate(case_id, case_members)
        if case_problems:
            reason, message = case_problems[0]
            selection.excluded.append(ExcludedCase(case_id=case_id, reason=reason, message=message))
            problems += [message for _, message in case_problems]
            continue
        selection.members += case_members
        selection.case_ids.append(case_id)

    if problems and strict:
        raise CaseResolutionError("\n".join(problems))

    for excluded in selection.excluded:
        LOGGER.warning("case %d excluded (%s): %s", excluded.case_id, excluded.reason, excluded.message)
    if problems and not strict:
        LOGGER.warning("%d candidate case(s) excluded, %d kept", len(selection.excluded), len(selection.case_ids))
    return selection


def decide_cases(fact_rows: list[dict], excluded_tenants: list[str]) -> list[Entry]:
    """One journal entry per case of `quality_control_facts_select.sql`: the decision and all its reasons."""
    members = _with_reasons(fact_rows, excluded_tenants)
    return decide(_group_by_case(members), excluded_tenants, _validate, Pipeline.QUALITY_CONTROL)


def decide_somatic_cases(fact_rows: list[dict], excluded_tenants: list[str]) -> list[Entry]:
    """One journal entry per case of `quality_control_somatic_facts_select.sql`."""
    members = _with_reasons(fact_rows, excluded_tenants)
    return decide(_group_by_case(members), excluded_tenants, _validate, Pipeline.QUALITY_CONTROL_SOMATIC)


def select_run_cases(member_rows: list[dict], excluded_tenants: list[str]) -> Selection:
    """The cases the scheduler decided `run` for (`quality_control_{,somatic_}case_members_select.sql`), checked
    again, lenient: the data may have moved since the decision. A member other than the proband with no CRAM yet is
    left out, and the family runs with the members present."""
    present = [m for m in _with_reasons(member_rows, excluded_tenants) if not is_absent(m)]
    return select_cases([m.model_dump() for m in present], strict=False)


def _with_reasons(rows: list[dict], excluded_tenants: list[str]) -> list[QcMember]:
    members = [QcMember(**row) for row in rows]
    for member in members:
        member.exclusion_reason = _query_reason(member, excluded_tenants)
    return members


def _query_reason(member: QcMember, excluded_tenants: list[str]) -> str | None:
    """A germline member's trigger document is its CRAM, a tumor's its DRAGEN metrics tarball: no CRAM-directory
    fallback for it, which would give a green run with an empty report."""
    if member.case_type == SOMATIC:
        reason = query_reason(member, excluded_tenants, "metrics", member.dragen_metrics_matches)
    else:
        reason = query_reason(member, excluded_tenants, "cram", member.cram_matches)
        if reason is None and member.dragen_metrics_matches > 1:
            reason = "ambiguous_metrics"
    # The shared code for two tarballs on one alignment.
    return "ambiguous_dragen_metrics" if reason == "ambiguous_metrics" else reason


def resolve_cases(member_dicts: list[dict]) -> list[QcCase]:
    """Build one `QcCase` per case. `select_cases` has already validated these members."""
    members = [QcMember(**m) for m in member_dicts]
    by_case = _group_by_case(members)
    return [_build(case_id, by_case[case_id]) for case_id in sorted(by_case)]


def tenants_of(cases: list[QcCase]) -> list[str]:
    """One batch PATCH per tenant; this is the only tenant-scoped step of the DAG."""
    return sorted({c.tenant_code for c in cases})


def strategy_of(member: QcMember) -> str:
    return STRATEGIES[member.strategy.lower()]


def _group_by_case(members: list[QcMember]) -> dict[int, list[QcMember]]:
    by_case: dict[int, list[QcMember]] = {}
    for member in members:
        by_case.setdefault(member.case_id, []).append(member)
    for case_members in by_case.values():
        case_members.sort(key=lambda m: (REL_ORDER.get(m.role, 9), m.patient_id))
    return by_case


def _validate(case_id: int, members: list[QcMember]) -> list[tuple[Reason, str]]:
    """`(reason, message)` pairs. Exclusion is at case granularity: the report is per family."""
    problems: list[tuple[Reason, str]] = []

    for member in members:
        if member.exclusion_reason:
            reason = from_query(member.exclusion_reason).about(member.patient_id)
            problems.append((reason, f"case {case_id}, patient {member.patient_id}: {reason.text}"))

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        problems.append(
            (
                Reason(code=ReasonCode.PROBAND_COUNT),
                f"case {case_id}: expected exactly 1 proband, found {len(probands)} -- the pedigree "
                f"the pipeline derives needs exactly one",
            )
        )

    unknown = sorted({m.strategy for m in members if m.strategy and m.strategy.lower() not in STRATEGIES})
    if unknown:
        problems.append(
            (
                Reason(code=ReasonCode.UNSUPPORTED_STRATEGY, detail=",".join(unknown)),
                f"case {case_id}: unsupported experimental strategy {unknown}",
            )
        )

    return problems


def _build(case_id: int, members: list[QcMember]) -> QcCase:
    proband = members[0]
    return QcCase(
        case_id=case_id,
        family_id=family_id(case_id),
        submitter_case_id=proband.submitter_case_id,
        tenant_code=proband.tenant_code,
        case_type=proband.case_type,
        project_code=proband.project_code,
        members=members,
    )
