"""Turn the CNV discovery result set into validated `CnvFamily` objects.

Same two-step shape as `radiant.tasks.nextflow.resolve`, for the same reasons: `select_cases`
decides which candidate cases can run and why the others cannot, in strict mode (an operator
named alignment tasks) or lenient mode (the nightly query found them); `resolve_families`
builds the families it kept. What differs is the trigger document -- a germline CNV VCF, not
a gVCF -- and one extra guard: the pipeline only knows DRAGEN's CNV conventions.
"""

import logging

from pydantic import BaseModel

from radiant.tasks.nextflow.cnv.model import CnvFamily, CnvMember, family_id
from radiant.tasks.nextflow.model import Phenotype
from radiant.tasks.nextflow.resolve import REL_ORDER, SEQUENCING_TYPES, CaseResolutionError, ExcludedCase

LOGGER = logging.getLogger(__name__)

# What the pipeline's samplesheet `caller` column accepts for a per-sample VCF. Matched
# case-insensitively as a prefix against `task.pipeline_name` ("Dragen", "DRAGEN 4.2", ...).
SUPPORTED_CALLER_PREFIX = "dragen"

REASON_TEXT = {
    "pending_sequencing": "has no completed sequencing experiment yet",
    "pending_alignment": "its current sequencing experiment has no alignment task yet",
    "no_gcnv": (
        "its current alignment published no germline CNV VCF (`gcnv`/`vcf`) -- either the case was "
        "joint-called upstream, or CNV calling was not run, and there is nothing to post-process"
    ),
    "ambiguous_gcnv": (
        "its current alignment published more than one germline CNV VCF. One alignment task "
        "cannot legitimately produce two, so a document is mistyped at the source -- typically an "
        "index recorded with format_code 'vcf' instead of 'tbi'"
    ),
    "no_project_code": (
        "the case has no project_code -- `cases.project_id` did not resolve to a row in "
        "`project`, and the batch PATCH requires it to look the case up"
    ),
    "tenant_not_granted": (
        "its tenant is not in the configured allow-list, so the batch PATCH would return 403 "
        "after the pipeline had already run"
    ),
    "unsupported_caller": (
        "its alignment was not produced by DRAGEN, and the pipeline's normalization step only "
        "implements DRAGEN's CNV VCF conventions -- it would fail on the first process"
    ),
    "unknown_caller": (
        "its alignment task has no pipeline_name, so the `caller=DRAGEN` the samplesheet requires "
        "cannot be asserted -- record the caller on the alignment task"
    ),
}

__all__ = ["CaseResolutionError", "ExcludedCase", "Selection", "select_cases", "resolve_families", "tenants_of"]


class Selection(BaseModel):
    """What `select_cases` decided: what runs, what does not, and the tenants involved."""

    members: list[CnvMember] = []
    case_ids: list[int] = []
    excluded: list[ExcludedCase] = []

    @property
    def tenants(self) -> list[str]:
        return sorted({m.tenant_code for m in self.members})


def select_cases(
    member_rows: list[dict],
    requested_task_ids: list[int] | None = None,
    *,
    strict: bool | None = None,
) -> Selection:
    """Group the candidate rows by case and keep the ones that can actually be run."""
    requested = list(requested_task_ids or [])
    if strict is None:
        strict = bool(requested)

    members = [CnvMember(**row) for row in member_rows]
    by_case = _group_by_case(members)
    problems: list[str] = []

    if requested:
        found = {m.alignment_task_id for m in members if m.alignment_task_id is not None}
        unmatched = sorted(set(requested) - found)
        if unmatched:
            problems.append(
                f"alignment task(s) {unmatched} produced no candidate case -- either the id does "
                f"not exist or is not an `alignment_germline_variant_calling` task, or every case "
                f"it belongs to already carries a `radiant_germline_cnv_annotation`"
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


def resolve_families(member_rows: list[dict], phenotype_rows: list[dict]) -> list[CnvFamily]:
    """Build one `CnvFamily` per case. `select_cases` has already validated these rows."""
    members = [CnvMember(**row) for row in member_rows]
    phenotypes = [Phenotype(**row) for row in phenotype_rows]
    by_case = _group_by_case(members)
    return [_build(case_id, by_case[case_id], phenotypes) for case_id in sorted(by_case)]


def tenants_of(families: list[CnvFamily]) -> list[str]:
    """One batch PATCH per tenant; this is the only tenant-scoped step of the DAG."""
    return sorted({f.tenant_code for f in families})


def _group_by_case(members: list[CnvMember]) -> dict[int, list[CnvMember]]:
    by_case: dict[int, list[CnvMember]] = {}
    for member in members:
        by_case.setdefault(member.case_id, []).append(member)
    for case_members in by_case.values():
        case_members.sort(key=lambda m: (REL_ORDER.get(m.role, 9), m.patient_id))
    return by_case


def _validate(case_id: int, members: list[CnvMember]) -> list[tuple[str, str]]:
    """`(reason, detail)` pairs. Exclusion is at case granularity: the pipeline merges and
    classifies whole families, so a family short one member is a different job."""
    problems: list[tuple[str, str]] = []

    for member in members:
        if member.exclusion_reason:
            text = REASON_TEXT.get(member.exclusion_reason, "is not usable")
            problems.append((member.exclusion_reason, f"case {case_id}, patient {member.patient_id}: {text}"))

    # Fails closed: the samplesheet states `caller=DRAGEN` as a fact, and `task.pipeline_name`
    # is nullable. An alignment that does not say what produced it is excluded and reported
    # rather than asserted to be DRAGEN and found out hours in, every night.
    for member in members:
        if member.alignment_task_id is None:
            continue  # already reported by the query (`pending_alignment`)
        pipeline = (member.alignment_pipeline or "").strip().lower()
        if not pipeline:
            problems.append(
                ("unknown_caller", f"case {case_id}, patient {member.patient_id}: {REASON_TEXT['unknown_caller']}")
            )
        elif not pipeline.startswith(SUPPORTED_CALLER_PREFIX):
            problems.append(
                (
                    "unsupported_caller",
                    f"case {case_id}, patient {member.patient_id}: {REASON_TEXT['unsupported_caller']} "
                    f"(alignment pipeline: {member.alignment_pipeline!r})",
                )
            )

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        problems.append(
            (
                "proband_count",
                f"case {case_id}: expected exactly 1 proband, found {len(probands)} -- two "
                f"different patients are marked proband on this case",
            )
        )

    strategies = {m.strategy for m in members if m.strategy}
    unknown = {s for s in strategies if s.lower() not in SEQUENCING_TYPES}
    if unknown:
        problems.append(
            ("unsupported_strategy", f"case {case_id}: unsupported experimental strategy {sorted(unknown)}")
        )
    elif len(strategies) > 1:
        # The samplesheet carries `sequencingType` per row, but Exomiser's analysis file is
        # chosen per family from it, so mixed strategies would be silently mis-analysed.
        problems.append(
            ("unsupported_strategy", f"case {case_id}: members span several strategies {sorted(strategies)}")
        )

    return problems


def _build(case_id: int, members: list[CnvMember], phenotypes: list[Phenotype]) -> CnvFamily:
    proband = members[0]
    return CnvFamily(
        case_id=case_id,
        family_id=family_id(case_id),
        submitter_case_id=proband.submitter_case_id,
        tenant_code=proband.tenant_code,
        project_code=proband.project_code,
        sequencing_type=SEQUENCING_TYPES.get(proband.strategy.lower(), proband.strategy.upper()),
        members=members,
        phenotypes=[p for p in phenotypes if p.case_id == case_id and p.patient_id == proband.patient_id],
    )
