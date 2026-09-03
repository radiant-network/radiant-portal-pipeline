"""Turn the discovery result set into validated `QcCase` objects.

Same two-step shape as `radiant.tasks.nextflow.resolve`, for the same reasons: `select_cases`
decides which candidate cases can run and why the others cannot, in strict mode (an operator
named alignment tasks) or lenient mode (the nightly query found them); `resolve_cases` builds
the cases it kept. What differs is the trigger document -- a CRAM, not a gVCF -- and that a
case may mix strategies, since the QC samplesheet carries `experimentalStrategy` per row.
"""

import logging

from pydantic import BaseModel

from radiant.tasks.nextflow.qc.model import DocumentRow, QcCase, QcMember, family_id
from radiant.tasks.nextflow.resolve import REL_ORDER, CaseResolutionError, ExcludedCase

LOGGER = logging.getLogger(__name__)

# The pipeline's `experimentalStrategy` enum. `wxs` and `wes` are the same thing under two
# dictionaries; the pipeline spells it WXS.
STRATEGIES = {"wgs": "WGS", "wxs": "WXS", "wes": "WXS"}

REASON_TEXT = {
    "pending_sequencing": "has no completed sequencing experiment yet",
    "pending_alignment": "its current sequencing experiment has no alignment task yet",
    "no_cram": "its current alignment published no CRAM, and somalier needs one",
    "ambiguous_cram": (
        "its current alignment published more than one CRAM. One alignment task cannot "
        "legitimately produce two, so a document is mistyped at the source -- typically an "
        "index recorded with format_code 'cram' instead of 'crai'"
    ),
    "no_project_code": (
        "the case has no project_code -- `cases.project_id` did not resolve to a row in "
        "`project`, and the batch PATCH requires it to look the case up"
    ),
    "tenant_not_granted": (
        "its tenant is not in the configured allow-list, so the batch PATCH would return 403 "
        "after the pipeline had already run"
    ),
    "no_dragen_metrics": (
        "no `<aliquot>.mapping_metrics.csv` was found in any directory holding one of its alignment's output documents"
    ),
    "ambiguous_dragen_metrics": (
        "`<aliquot>.mapping_metrics.csv` was found in more than one of the directories holding "
        "its alignment's output documents, and the pipeline would attach both"
    ),
    "metrics_not_on_workspace": (
        "its DRAGEN metrics are not in the workspace bucket, so they are not on the shared "
        "filesystem the pipeline reads"
    ),
    "metrics_dir_split": (
        "its members' DRAGEN metrics sit in different directories, and their common ancestor "
        "either leaves the workspace bucket or holds a duplicated sample -- one Nextflow run "
        "takes exactly one `--dragen_metrics_dir`"
    ),
}

__all__ = ["CaseResolutionError", "ExcludedCase", "Selection", "select_cases", "resolve_cases", "tenants_of"]


class Selection(BaseModel):
    """What `select_cases` decided: what runs, what does not, and the tenants involved."""

    members: list[QcMember] = []
    case_ids: list[int] = []
    excluded: list[ExcludedCase] = []

    @property
    def tenants(self) -> list[str]:
        return sorted({m.tenant_code for m in self.members})


def fold_rows(rows: list[dict]) -> list[QcMember]:
    """One `QcMember` per (case, patient), its document rows folded into `document_urls`."""
    members: dict[tuple[int, int], QcMember] = {}
    urls: dict[tuple[int, int], set[str]] = {}
    for raw in rows:
        row = DocumentRow(**raw)
        key = (row.case_id, row.patient_id)
        if key not in members:
            fields = row.model_dump(exclude={"document_url", "document_data_type", "document_format"})
            members[key] = QcMember(**fields)
            urls[key] = set()
        if row.document_url:
            urls[key].add(row.document_url)
    for key, member in members.items():
        member.document_urls = sorted(urls[key])
    return list(members.values())


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

    members = fold_rows(rows)
    by_case = _group_by_case(members)
    problems: list[str] = []

    if requested:
        found = {m.alignment_task_id for m in members if m.alignment_task_id is not None}
        unmatched = sorted(set(requested) - found)
        if unmatched:
            problems.append(
                f"alignment task(s) {unmatched} produced no candidate case -- either the id does "
                f"not exist or is not an `alignment_germline_variant_calling` task, or every case "
                f"it belongs to already carries a `quality_control_metrics` task"
            )

    selection = Selection()
    for case_id in sorted(by_case):
        case_members = by_case[case_id]
        case_problems = _validate(case_id, case_members)
        if case_problems:
            reason, detail = case_problems[0]
            selection.excluded.append(ExcludedCase(case_id=case_id, reason=reason, detail=detail))
            problems += [detail for _, detail in case_problems]
            continue
        selection.members += case_members
        selection.case_ids.append(case_id)

    if problems and strict:
        raise CaseResolutionError("\n".join(problems))

    for excluded in selection.excluded:
        LOGGER.warning("case %d excluded (%s): %s", excluded.case_id, excluded.reason, excluded.detail)
    if problems and not strict:
        LOGGER.warning("%d candidate case(s) excluded, %d kept", len(selection.excluded), len(selection.case_ids))
    return selection


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


def _validate(case_id: int, members: list[QcMember]) -> list[tuple[str, str]]:
    """`(reason, detail)` pairs. Exclusion is at case granularity: the report is per family,
    and a family short one member is a different report, not a smaller one."""
    problems: list[tuple[str, str]] = []

    for member in members:
        if member.exclusion_reason:
            text = REASON_TEXT.get(member.exclusion_reason, "is not usable")
            problems.append((member.exclusion_reason, f"case {case_id}, patient {member.patient_id}: {text}"))

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        problems.append(
            (
                "proband_count",
                f"case {case_id}: expected exactly 1 proband, found {len(probands)} -- the pedigree "
                f"the pipeline derives needs exactly one",
            )
        )

    unknown = sorted({m.strategy for m in members if m.strategy and m.strategy.lower() not in STRATEGIES})
    if unknown:
        problems.append(("unsupported_strategy", f"case {case_id}: unsupported experimental strategy {unknown}"))

    return problems


def _build(case_id: int, members: list[QcMember]) -> QcCase:
    proband = members[0]
    return QcCase(
        case_id=case_id,
        family_id=family_id(case_id),
        submitter_case_id=proband.submitter_case_id,
        tenant_code=proband.tenant_code,
        project_code=proband.project_code,
        members=members,
    )
