"""Turn the CNV discovery result set into validated `CnvFamily` objects.

Same two-step shape as `radiant.tasks.nextflow.germline_snv.resolve`, for the same reasons:
`select_cases` decides every case from the fact rows, in strict mode (an operator named alignment
tasks) or lenient mode (a discovery run); `resolve_families` builds the families it kept. What
differs is the trigger document -- a germline CNV VCF, not a gVCF -- and one extra guard: the
pipeline only knows DRAGEN's CNV conventions.
"""

from pydantic import BaseModel

from radiant.tasks.nextflow.family import Phenotype, family_id
from radiant.tasks.nextflow.germline_cnv.model import CnvFamily, CnvMember
from radiant.tasks.nextflow.problem import Problem, case_problem, member_problem
from radiant.tasks.nextflow.selection import (
    SEQUENCING_TYPES,
    CaseResolutionError,
    ExcludedCase,
    case_problems,
    decide_cases,
    document_reason,
    group_by_case,
)

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
    """What `select_cases` decided: what runs, what does not, and the tenants involved
    (`selection.CaseDecisions`)."""

    members: list[CnvMember] = []
    case_ids: list[int] = []
    excluded: list[ExcludedCase] = []
    done: list[int] = []
    untriggered: list[int] = []

    @property
    def tenants(self) -> list[str]:
        return sorted({m.tenant_code for m in self.members})


def select_cases(
    member_rows: list[dict],
    requested_task_ids: list[int] | None = None,
    *,
    strict: bool | None = None,
    excluded_tenants: list[str] | None = None,
) -> Selection:
    """Decide every germline case of the fact rows: run, done, untriggered, or excluded with
    its reasons. The work is a member's CNV VCF not annotated on the case yet."""
    blocked = set(excluded_tenants or [])
    decisions = decide_cases(
        [CnvMember(**row) for row in member_rows],
        triggered=lambda m: m.gcnv_matches > 0,
        validate=lambda case_id, members: _validate(case_id, members, blocked),
        requested_task_ids=requested_task_ids,
        strict=strict,
        unmatched_text=(
            "either the id does not exist or is not an `alignment_germline_variant_calling` task, "
            "or every case it belongs to already carries a `radiant_germline_cnv_annotation`"
        ),
    )
    return Selection(**decisions.model_dump())


def resolve_families(member_rows: list[dict], phenotype_rows: list[dict]) -> list[CnvFamily]:
    """Build one `CnvFamily` per case. `select_cases` has already validated these rows."""
    members = [CnvMember(**row) for row in member_rows]
    phenotypes = [Phenotype(**row) for row in phenotype_rows]
    by_case = group_by_case(members)
    return [_build(case_id, by_case[case_id], phenotypes) for case_id in sorted(by_case)]


def tenants_of(families: list[CnvFamily]) -> list[str]:
    """One batch PATCH per tenant; this is the only tenant-scoped step of the DAG."""
    return sorted({f.tenant_code for f in families})


def _validate(case_id: int, members: list[CnvMember], excluded_tenants: set[str]) -> list[Problem]:
    """Every problem of a candidate case. Exclusion is at case granularity: the pipeline merges
    and classifies whole families, so a family short one member is a different job."""
    problems: list[Problem] = []

    reasons = {m.patient_id: document_reason(m, m.gcnv_matches, "no_gcnv", "ambiguous_gcnv") for m in members}
    for member in members:
        reason = reasons[member.patient_id]
        if reason:
            detail = f"case {case_id}, patient {member.patient_id}: {REASON_TEXT[reason]}"
            problems.append(member_problem(member, reason, detail))

    problems += case_problems(case_id, members[0], excluded_tenants)

    # The aliquot is every id the pipeline sees (samplesheet, PED, phenopacket), and the
    # `<proband>` segment of the Exomiser report names `collect_outputs` expects.
    for member in members:
        if not reasons[member.patient_id] and not member.aliquot:
            detail = f"case {case_id}, patient {member.patient_id}: its sequencing experiment has no aliquot"
            problems.append(member_problem(member, "missing_aliquot", detail))

    # Fails closed: the samplesheet states `caller=DRAGEN` as a fact, and `task.pipeline_name`
    # is nullable. An alignment that does not say what produced it is excluded and reported
    # rather than asserted to be DRAGEN and found out hours in, every night.
    for member in members:
        if reasons[member.patient_id]:
            continue
        pipeline = (member.alignment_pipeline or "").strip().lower()
        if not pipeline:
            problems.append(
                member_problem(
                    member,
                    "unknown_caller",
                    f"case {case_id}, patient {member.patient_id}: {REASON_TEXT['unknown_caller']}",
                )
            )
        elif not pipeline.startswith(SUPPORTED_CALLER_PREFIX):
            problems.append(
                member_problem(
                    member,
                    "unsupported_caller",
                    f"case {case_id}, patient {member.patient_id}: {REASON_TEXT['unsupported_caller']} "
                    f"(alignment pipeline: {member.alignment_pipeline!r})",
                )
            )

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        problems.append(
            case_problem(
                "proband_count",
                f"case {case_id}: expected exactly 1 proband, found {len(probands)} -- two "
                f"different patients are marked proband on this case",
            )
        )

    strategies = {m.strategy for m in members if m.strategy}
    unknown = {s for s in strategies if s.lower() not in SEQUENCING_TYPES}
    if unknown:
        problems.append(
            case_problem(
                "unsupported_strategy", f"case {case_id}: unsupported experimental strategy {sorted(unknown)}"
            )
        )
    elif len(strategies) > 1:
        # The samplesheet carries `sequencingType` per row, but Exomiser's analysis file is
        # chosen per family from it, so mixed strategies would be silently mis-analysed.
        problems.append(
            case_problem(
                "unsupported_strategy", f"case {case_id}: members span several strategies {sorted(strategies)}"
            )
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
