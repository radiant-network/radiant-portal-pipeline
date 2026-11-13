"""Turn the discovery result set into validated `QcCase` objects.

Same two-step shape as `radiant.tasks.nextflow.germline_snv.resolve`, for the same reasons: `select_cases`
decides which candidate cases can run and why the others cannot, in strict mode (an operator
named alignment tasks) or lenient mode (the nightly query found them); `resolve_cases` builds
the cases it kept. What differs is the trigger document -- a CRAM, not a gVCF; for a somatic
member, the DRAGEN metrics tarball, with no CRAM -- that a case may mix strategies, since the QC
samplesheet carries `experimentalStrategy` per row, and that a somatic case is one member, its
proband's tumor.
"""

from pydantic import BaseModel

from radiant.tasks.nextflow.family import family_id
from radiant.tasks.nextflow.problem import Problem, case_problem, member_problem
from radiant.tasks.nextflow.qc.model import SOMATIC, QcCase, QcMember
from radiant.tasks.nextflow.selection import (
    CaseResolutionError,
    ExcludedCase,
    case_problems,
    decide_cases,
    document_reason,
    group_by_case,
)

# The pipeline's `experimentalStrategy` enum. `wxs` and `wes` are the same thing under two
# dictionaries; the pipeline spells it WXS.
STRATEGIES = {"wgs": "WGS", "wxs": "WXS", "wes": "WXS"}

REASON_TEXT = {
    "pending_sequencing": "has no completed sequencing experiment yet",
    "pending_alignment": "its current sequencing experiment has no alignment task yet",
    "no_cram": "its current germline alignment published no CRAM, and somalier needs one",
    "ambiguous_cram": (
        "its current alignment published more than one CRAM. One alignment task cannot "
        "legitimately produce two, so a document is mistyped at the source -- typically an "
        "index recorded with format_code 'cram' instead of 'crai'"
    ),
    "ambiguous_dragen_metrics": (
        "its current alignment published more than one DRAGEN metrics tarball (`ssup`/`tgz`), "
        "and the samplesheet takes exactly one per sample"
    ),
    "no_metrics": (
        "its current somatic alignment published no DRAGEN metrics tarball (`ssup`/`tgz`). A tumor "
        "row has no CRAM-directory fallback: the pipeline would give a green run with an empty report"
    ),
}

__all__ = ["CaseResolutionError", "ExcludedCase", "Selection", "select_cases", "resolve_cases", "tenants_of"]


class Selection(BaseModel):
    """What `select_cases` decided: what runs, what does not, and the tenants involved
    (`selection.CaseDecisions`)."""

    members: list[QcMember] = []
    case_ids: list[int] = []
    excluded: list[ExcludedCase] = []
    done: list[int] = []
    untriggered: list[int] = []

    @property
    def tenants(self) -> list[str]:
        return sorted({m.tenant_code for m in self.members})


def select_cases(
    rows: list[dict],
    requested_task_ids: list[int] | None = None,
    *,
    strict: bool | None = None,
    excluded_tenants: list[str] | None = None,
) -> Selection:
    """Decide every germline and somatic case of the fact rows: run, done, untriggered, or excluded
    with its reasons. The work is a germline member's CRAM, or a somatic tumor's alignment, not
    QC'd on the case yet."""
    blocked = set(excluded_tenants or [])
    decisions = decide_cases(
        [QcMember(**row) for row in rows],
        triggered=_triggered,
        validate=lambda case_id, members: _validate(case_id, members, blocked),
        requested_task_ids=requested_task_ids,
        strict=strict,
        unmatched_text=(
            "either the id does not exist, is not the current `alignment_germline_variant_calling` or "
            "`alignment_somatic_variant_calling` task of its experiment, or every case it belongs to "
            "already carries a `quality_control_metrics` task"
        ),
    )
    return Selection(**decisions.model_dump())


def resolve_cases(member_dicts: list[dict]) -> list[QcCase]:
    """Build one `QcCase` per case. `select_cases` has already validated these members."""
    members = [QcMember(**m) for m in member_dicts]
    by_case = group_by_case(members)
    return [_build(case_id, by_case[case_id]) for case_id in sorted(by_case)]


def tenants_of(cases: list[QcCase]) -> list[str]:
    """One batch PATCH per tenant; this is the only tenant-scoped step of the DAG."""
    return sorted({c.tenant_code for c in cases})


def strategy_of(member: QcMember) -> str:
    return STRATEGIES[member.strategy.lower()]


def _triggered(member: QcMember) -> bool:
    if member.case_type == SOMATIC:
        return member.alignment_task_id is not None
    return member.cram_matches > 0


def _member_reason(member: QcMember) -> str | None:
    if member.case_type == SOMATIC:
        # No CRAM-directory fallback for a tumor: it would give a green run with an empty report.
        return document_reason(member, member.dragen_metrics_matches, "no_metrics", "ambiguous_dragen_metrics")
    reason = document_reason(member, member.cram_matches, "no_cram", "ambiguous_cram")
    if reason is None and member.dragen_metrics_matches > 1:
        return "ambiguous_dragen_metrics"
    return reason


def _validate(case_id: int, members: list[QcMember], excluded_tenants: set[str]) -> list[Problem]:
    """Every problem of a candidate case. Exclusion is at case granularity: the report is per
    family, and a family short one member is a different report, not a smaller one."""
    problems: list[Problem] = []

    for member in members:
        reason = _member_reason(member)
        if reason:
            detail = f"case {case_id}, patient {member.patient_id}: {REASON_TEXT[reason]}"
            problems.append(member_problem(member, reason, detail))

    problems += case_problems(case_id, members[0], excluded_tenants)

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        problems.append(
            case_problem(
                "proband_count",
                f"case {case_id}: expected exactly 1 proband, found {len(probands)} -- the pedigree "
                f"the pipeline derives needs exactly one",
            )
        )

    unknown = sorted({m.strategy for m in members if m.strategy and m.strategy.lower() not in STRATEGIES})
    if unknown:
        problems.append(
            case_problem("unsupported_strategy", f"case {case_id}: unsupported experimental strategy {unknown}")
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
