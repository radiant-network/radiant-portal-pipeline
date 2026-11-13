"""Turn the germline SNV fact rows into decisions and validated `SnvFamily` objects.

Two steps, deliberately separate. `select_cases` decides every case (run, done, no trigger
document, or excluded and why); the fact query decides nothing. `resolve_families` builds the
families it kept. Splitting them is what lets the same checks behave differently depending on
how the scope was chosen:

- **strict** -- an operator named specific alignment tasks, so a case that cannot be
  resolved is an error they must see;
- **lenient** -- the scope was discovered by the nightly query, so a case that cannot be
  resolved is dropped and reported, and every other case still runs.

A samplesheet that is merely *plausible* costs hours of pipeline time before anyone finds
out, and an unnoticed mis-attributed gVCF costs more than that.
"""

import logging

from pydantic import BaseModel

from radiant.tasks.nextflow.family import Phenotype, family_id
from radiant.tasks.nextflow.germline_snv.model import SnvFamily, SnvMember
from radiant.tasks.nextflow.problem import Problem, case_problem, member_problem
from radiant.tasks.nextflow.selection import REL_ORDER, SEQUENCING_TYPES, CaseResolutionError, ExcludedCase

LOGGER = logging.getLogger(__name__)

# The reasons, in the wording used when reporting them.
REASON_TEXT = {
    "pending_sequencing": "has no completed sequencing experiment yet",
    "pending_alignment": "its current sequencing experiment has no alignment task yet",
    "no_gvcf": (
        "its current alignment published no gVCF -- the case was joint-called upstream and is "
        "out of scope for this DAG, there is nothing for `step: genotype` to do"
    ),
    "ambiguous_gvcf": (
        "its current alignment published more than one gVCF. One alignment task cannot "
        "legitimately produce two, so a document is mistyped at the source -- typically an "
        "index recorded with format_code 'gvcf' instead of 'tbi'"
    ),
    "no_project_code": (
        "the case has no project_code -- `cases.project_id` did not resolve to a row in "
        "`project`, and the batch PATCH requires it to look the case up"
    ),
    "tenant_excluded": (
        "its tenant is in `excluded_tenants` (`$RADIANT_ETL_EXCLUDED_TENANTS`), which the ETL "
        "leaves out of case processing"
    ),
    "no_trigger_document": (
        "no member's current alignment published a gVCF -- joint-called upstream, nothing for this DAG to do"
    ),
}


class Selection(BaseModel):
    """What `select_cases` decided: what runs, what does not, and the tenants involved.

    `done`: annotated, and no member's gVCF is left to annotate. `untriggered`: never annotated and
    no member has a gVCF. Both are left empty on a targeted run, which only looks at the requested
    tasks.
    """

    members: list[SnvMember] = []
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
    its reasons.

    `strict` defaults to "an operator asked for specific tasks", i.e. to whether
    `requested_task_ids` is non-empty.
    """
    requested = set(requested_task_ids or [])
    if strict is None:
        strict = bool(requested)
    excluded_tenants = set(excluded_tenants or [])

    members = [SnvMember(**row) for row in member_rows]
    by_case = _group_by_case(members)

    selection = Selection()
    candidates: dict[int, list[SnvMember]] = {}
    for case_id in sorted(by_case):
        case_members = by_case[case_id]
        # A member's gVCF not annotated on this case yet is the work.
        pending = [m for m in case_members if m.gvcf_matches > 0 and not m.annotated]
        if requested:
            # A targeted run only looks at the cases a requested task still has work for.
            if any(m.alignment_task_id in requested for m in pending):
                candidates[case_id] = case_members
        elif pending:
            candidates[case_id] = case_members
        elif any(m.annotated for m in case_members):
            selection.done.append(case_id)
        else:
            selection.untriggered.append(case_id)

    problems: list[str] = []

    # A requested task that gave no candidate case: it does not exist, is not an alignment task,
    # or every case it belongs to is already annotated.
    if requested:
        found = {m.alignment_task_id for ms in candidates.values() for m in ms if m.alignment_task_id is not None}
        unmatched = sorted(requested - found)
        if unmatched:
            problems.append(
                f"alignment task(s) {unmatched} produced no candidate case -- either the id does "
                f"not exist or is not an `alignment_germline_variant_calling` task, or every case "
                f"it belongs to already carries a `radiant_germline_annotation`"
            )

    for case_id, case_members in candidates.items():
        case_problems = _validate(case_id, case_members, excluded_tenants)
        if case_problems:
            first = case_problems[0]
            selection.excluded.append(
                ExcludedCase(case_id=case_id, reason=first.reason, detail=first.detail, problems=case_problems)
            )
            problems += [problem.detail for problem in case_problems]
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


def resolve_families(member_rows: list[dict], phenotype_rows: list[dict]) -> list[SnvFamily]:
    """Build one `SnvFamily` per case. `select_cases` has already validated these rows."""
    members = [SnvMember(**row) for row in member_rows]
    phenotypes = [Phenotype(**row) for row in phenotype_rows]
    by_case = _group_by_case(members)
    return [_build(case_id, by_case[case_id], phenotypes) for case_id in sorted(by_case)]


def tenants_of(families: list[SnvFamily]) -> list[str]:
    """The tenants these cases belong to, sorted.

    A run may span several. The batch endpoint is `PATCH /{tenant}/cases/batch`, one tenant
    per call, and that is the *only* thing in the DAG that is tenant-scoped -- reads are one
    shared schema and the pipeline is tenant-blind. So registration groups by this and sends
    one batch each, rather than the run being split into several pipeline runs that would
    then serialise behind `max_active_runs=1` for no structural reason.
    """
    return sorted({f.tenant_code for f in families})


def _group_by_case(members: list[SnvMember]) -> dict[int, list[SnvMember]]:
    by_case: dict[int, list[SnvMember]] = {}
    for member in members:
        by_case.setdefault(member.case_id, []).append(member)
    for case_members in by_case.values():
        case_members.sort(key=lambda m: (REL_ORDER.get(m.role, 9), m.patient_id))
    return by_case


def member_reason(member: SnvMember) -> str | None:
    """Why this member keeps its case from running, most specific first: a member with no
    experiment has no alignment either. The two `pending_*` are transient: the case waits."""
    if member.seq_id is None:
        return "pending_sequencing"
    if member.alignment_task_id is None:
        return "pending_alignment"
    if member.gvcf_matches == 0:
        return "no_gvcf"
    if member.gvcf_matches > 1:
        return "ambiguous_gvcf"
    return None


def _validate(case_id: int, members: list[SnvMember], excluded_tenants: set[str]) -> list[Problem]:
    """Every problem of a candidate case. Exclusion is at case granularity: the pipeline
    joint-calls whole families, so a family missing a member is not a smaller job, it is a
    different one.
    """
    problems: list[Problem] = []

    reasons = {member.patient_id: member_reason(member) for member in members}
    for member in members:
        reason = reasons[member.patient_id]
        if reason:
            detail = f"case {case_id}, patient {member.patient_id}: {REASON_TEXT[reason]}"
            problems.append(member_problem(member, reason, detail))

    case = members[0]
    if case.project_code is None:
        problems.append(case_problem("no_project_code", f"case {case_id}: {REASON_TEXT['no_project_code']}"))
    if case.tenant_code in excluded_tenants:
        problems.append(case_problem("tenant_excluded", f"case {case_id}: {REASON_TEXT['tenant_excluded']}"))

    # The aliquot is every id the pipeline sees (samplesheet, PED, phenopacket): the gVCF
    # sample name. A completed experiment always has one; this names it should that change.
    for member in members:
        if not reasons[member.patient_id] and not member.aliquot:
            detail = f"case {case_id}, patient {member.patient_id}: its sequencing experiment has no aliquot"
            problems.append(member_problem(member, "missing_aliquot", detail))

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        # Now means what it says. Before one experiment per member was selected, a
        # re-sequenced proband produced two rows and tripped this on a cause it never named.
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
        problems.append(
            case_problem(
                "unsupported_strategy", f"case {case_id}: members span several strategies {sorted(strategies)}"
            )
        )

    return problems


def _build(case_id: int, members: list[SnvMember], phenotypes: list[Phenotype]) -> SnvFamily:
    proband = members[0]
    return SnvFamily(
        case_id=case_id,
        family_id=family_id(case_id),
        submitter_case_id=proband.submitter_case_id,
        tenant_code=proband.tenant_code,
        project_code=proband.project_code,
        sequencing_type=SEQUENCING_TYPES.get(proband.strategy.lower(), proband.strategy.upper()),
        members=members,
        phenotypes=[p for p in phenotypes if p.case_id == case_id and p.patient_id == proband.patient_id],
    )
