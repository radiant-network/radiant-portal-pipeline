"""Turn the two clinical query result sets into validated `Family` objects.

Every check here fails the run rather than producing a quietly wrong samplesheet. A
samplesheet that is merely *plausible* costs hours of pipeline time before anyone finds
out, and an unnoticed mis-attributed gVCF costs more than that.
"""

from radiant.tasks.nextflow.model import CaseMember, Family, Phenotype, family_id

# Order members are emitted in: proband first, then parents, then everyone else.
REL_ORDER = {"proband": 0, "father": 1, "mother": 2, "brother": 3, "sister": 3, "sibling": 3, "fetus": 4}

# The pipeline's `sequencingType` column, uppercase. `wxs` and `wes` are the same thing
# under two dictionaries; the pipeline only knows WES.
SEQUENCING_TYPES = {"wgs": "WGS", "wxs": "WES", "wes": "WES"}


class CaseResolutionError(Exception):
    """One or more requested cases cannot be turned into a valid pipeline input."""


def resolve_families(
    member_rows: list[dict],
    phenotype_rows: list[dict],
    case_ids: list[int],
) -> list[Family]:
    """Group, order and validate. Raises `CaseResolutionError` listing every problem."""
    members = [CaseMember(**row) for row in member_rows]
    phenotypes = [Phenotype(**row) for row in phenotype_rows]

    by_case: dict[int, list[CaseMember]] = {}
    for member in members:
        by_case.setdefault(member.case_id, []).append(member)
    for case_members in by_case.values():
        case_members.sort(key=lambda m: (REL_ORDER.get(m.role, 9), m.patient_id))

    problems: list[str] = []

    # A requested id absent from the result set is either unknown or somatic. The query
    # cannot tell us which, but naming the id is enough to go and look.
    missing = [cid for cid in case_ids if cid not in by_case]
    if missing:
        problems.append(
            f"no germline case for id(s) {sorted(missing)} -- either the id does not exist, "
            f"or it is a somatic case (the pipeline's `step: genotype` entry point assumes "
            f"germline joint calling)"
        )

    # The batch PATCH is addressed to one tenant, so a mixed set would need two calls.
    # Rejecting is better than silently registering half of them: split the run instead.
    tenants = {m.tenant_code for m in members}
    if len(tenants) > 1:
        problems.append(f"the requested cases span several tenants {sorted(tenants)}; run them one tenant at a time")

    families = []
    for case_id in sorted(by_case):
        case_members = by_case[case_id]
        problems += _validate(case_id, case_members)
        families.append(_build(case_id, case_members, phenotypes))

    if problems:
        raise CaseResolutionError("\n".join(problems))

    return families


def tenant_of(families: list[Family]) -> str:
    """The tenant these cases belong to. `resolve_families` has already guaranteed one."""
    return families[0].tenant_code


def _validate(case_id: int, members: list[CaseMember]) -> list[str]:
    problems = []

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        problems.append(f"case {case_id}: expected exactly 1 proband, found {len(probands)}")

    # The batch PATCH resolves a case by (project_code, submitter_case_id) and rejects a
    # null project_code outright. Catching it here costs a query; catching it at
    # register_tasks costs a whole pipeline run first.
    if not members[0].project_code:
        problems.append(
            f"case {case_id}: no project_code -- `cases.project_id` did not resolve to a row in "
            f"`project`. The batch PATCH requires it to look the case up"
        )

    for member in members:
        if member.gvcf_matches == 1:
            continue
        if member.gvcf_matches == 0:
            problems.append(
                f"case {case_id}, sample {member.sample_id}: no gVCF registered. "
                f"Cases joint-called upstream have no per-sample gVCFs and are out of scope "
                f"for this DAG -- there is nothing for `step: genotype` to do"
            )
        else:
            problems.append(
                f"case {case_id}, sample {member.sample_id}: {member.gvcf_matches} gVCFs match, "
                f"expected 1. Either the alignment task spans several members ambiguously, or a "
                f"document is mistyped at the source (an index recorded with format_code 'gvcf', "
                f"for instance)"
            )

    strategies = {m.strategy for m in members}
    unknown = {s for s in strategies if s.lower() not in SEQUENCING_TYPES}
    if unknown:
        problems.append(f"case {case_id}: unsupported experimental strategy {sorted(unknown)}")
    elif len(strategies) > 1:
        problems.append(f"case {case_id}: members span several strategies {sorted(strategies)}")

    return problems


def _build(case_id: int, members: list[CaseMember], phenotypes: list[Phenotype]) -> Family:
    proband = members[0]
    return Family(
        case_id=case_id,
        family_id=family_id(case_id),
        submitter_case_id=proband.submitter_case_id,
        tenant_code=proband.tenant_code,
        project_code=proband.project_code,
        sequencing_type=SEQUENCING_TYPES.get(proband.strategy.lower(), proband.strategy.upper()),
        members=members,
        phenotypes=[p for p in phenotypes if p.case_id == case_id and p.patient_id == proband.patient_id],
    )
