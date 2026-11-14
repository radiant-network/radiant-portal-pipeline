"""Turn the clinical query result set into validated `Family` objects.

Two steps, deliberately separate. `select_cases` decides which candidate cases can be run
and why the others cannot; `resolve_families` builds the families it kept. Splitting them
is what lets the same checks behave differently depending on how the scope was chosen:

- **strict** -- an operator named specific alignment tasks, so a case that cannot be
  resolved is an error they must see;
- **lenient** -- the scope was discovered by the nightly query, so a case that cannot be
  resolved is dropped and reported, and every other case still runs.

Dropping the checks in lenient mode and trusting the query instead was considered and
rejected: the two sets of predicates would drift, and the day they disagree the failure is
a wrong samplesheet rather than an error. These are a backstop behind the query, not a
replacement for it.

A samplesheet that is merely *plausible* costs hours of pipeline time before anyone finds
out, and an unnoticed mis-attributed gVCF costs more than that.
"""

import logging

from pydantic import BaseModel

from radiant.tasks.decision import Decision, Reason, ReasonCode, decision_of, from_query
from radiant.tasks.journal import Entry, Pipeline
from radiant.tasks.nextflow.model import CaseMember, Family, Phenotype, family_id

LOGGER = logging.getLogger(__name__)

# Order members are emitted in: proband first, then parents, then everyone else.
REL_ORDER = {"proband": 0, "father": 1, "mother": 2, "brother": 3, "sister": 3, "sibling": 3, "fetus": 4}

# The pipeline's `sequencingType` column, uppercase. `wxs` and `wes` are the same thing
# under two dictionaries; the pipeline only knows WES.
SEQUENCING_TYPES = {"wgs": "WGS", "wxs": "WES", "wes": "WES"}


class CaseResolutionError(Exception):
    """One or more requested cases cannot be turned into a valid pipeline input."""


class ExcludedCase(BaseModel):
    """A candidate case that will not be run, and why."""

    case_id: int
    reason: Reason
    message: str


class Selection(BaseModel):
    """What `select_cases` decided: what runs, what does not, and the tenants involved."""

    members: list[CaseMember] = []
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
    """Group the candidate rows by case and keep the ones that can actually be run.

    `strict` defaults to "an operator asked for specific tasks", i.e. to whether
    `requested_task_ids` is non-empty.
    """
    requested = list(requested_task_ids or [])
    if strict is None:
        strict = bool(requested)

    members = [CaseMember(**row) for row in member_rows]
    by_case = _group_by_case(members)

    problems: list[str] = []

    # A requested task that produced no candidate case. The query filters candidacy by task
    # id, so this means the task does not exist, is not an alignment task, or every case it
    # belongs to is already annotated -- worth naming when someone asked for it by hand, and
    # meaningless on a discovery run where nobody did.
    if requested:
        found = {m.alignment_task_id for m in members if m.alignment_task_id is not None}
        unmatched = sorted(set(requested) - found)
        if unmatched:
            problems.append(
                f"alignment task(s) {unmatched} produced no candidate case -- either the id does "
                f"not exist or is not an `alignment_germline_variant_calling` task, or every case "
                f"it belongs to already carries a `radiant_germline_annotation`"
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


# A member not sequenced or not aligned yet: absent from the run, unless it is the proband.
_ABSENT = {"pending_sequencing", "pending_alignment"}


def select_run_cases(member_rows: list[dict], excluded_tenants: list[str]) -> Selection:
    """The families of the cases the scheduler decided `run` for (`germline_snv_case_members_select.sql`), checked
    again, lenient: the data may have moved since the decision. A member other than the proband with no gVCF yet is
    left out, and the family runs with the members present."""
    # The members query has the gVCF's alignment only: a member without one is not aligned, as far as it can tell.
    rows = [{"current_alignment_task_id": row.get("alignment_task_id")} | row for row in member_rows]
    members = _with_reasons(rows, excluded_tenants)
    present = [m for m in members if not _absent(m)]
    return select_cases([m.model_dump() for m in present], strict=False)


def decide_cases(fact_rows: list[dict], excluded_tenants: list[str]) -> list[Entry]:
    """One journal entry per case of `germline_snv_facts_select.sql`: the decision and all its reasons.

    A member absent but the proband does not block: the case runs with the members present, as long as one of
    them is not annotated yet, and runs again once the absent member arrives. Otherwise it waits for them.
    """
    entries = []
    for case_id, case_members in sorted(_group_by_case(_with_reasons(fact_rows, excluded_tenants)).items()):
        present = [m for m in case_members if not _absent(m)]
        absent = [from_query(m.exclusion_reason).about(m.patient_id) for m in case_members if _absent(m)]
        blocking = [reason for reason, _ in _validate(case_id, present)]
        # Case-level reasons count even when a member already has one: a tenant left out is a skip.
        first = case_members[0]
        if first.project_code is None:
            blocking.append(Reason(code=ReasonCode.NO_PROJECT_CODE))
        if first.tenant_code in excluded_tenants:
            blocking.append(Reason(code=ReasonCode.TENANT_EXCLUDED))

        reasons = tuple(dict.fromkeys([*blocking, *absent]))
        if blocking:
            decision = decision_of(blocking)
        elif any(not m.annotated for m in present):
            decision = Decision.RUN
        elif absent:
            decision = Decision.WAIT
        else:
            # Every member present is annotated and none is missing: nothing to decide.
            continue
        entries.append(Entry(case_id=case_id, pipeline=Pipeline.GERMLINE_SNV, decision=decision, reasons=reasons))
    return entries


def _with_reasons(fact_rows: list[dict], excluded_tenants: list[str]) -> list[CaseMember]:
    members = [CaseMember(**row) for row in fact_rows]
    for member in members:
        member.exclusion_reason = _query_reason(member, excluded_tenants)
    return members


def _absent(member: CaseMember) -> bool:
    return member.role != "proband" and member.exclusion_reason in _ABSENT


def resolve_families(member_rows: list[dict], phenotype_rows: list[dict]) -> list[Family]:
    """Build one `Family` per case. `select_cases` has already validated these rows."""
    members = [CaseMember(**row) for row in member_rows]
    phenotypes = [Phenotype(**row) for row in phenotype_rows]
    by_case = _group_by_case(members)
    return [_build(case_id, by_case[case_id], phenotypes) for case_id in sorted(by_case)]


def tenants_of(families: list[Family]) -> list[str]:
    """The tenants these cases belong to, sorted.

    A run may span several. The batch endpoint is `PATCH /{tenant}/cases/batch`, one tenant
    per call, and that is the *only* thing in the DAG that is tenant-scoped -- reads are one
    shared schema and the pipeline is tenant-blind. So registration groups by this and sends
    one batch each, rather than the run being split into several pipeline runs that would
    then serialise behind `max_active_runs=1` for no structural reason.
    """
    return sorted({f.tenant_code for f in families})


def _group_by_case(members: list[CaseMember]) -> dict[int, list[CaseMember]]:
    by_case: dict[int, list[CaseMember]] = {}
    for member in members:
        by_case.setdefault(member.case_id, []).append(member)
    for case_members in by_case.values():
        case_members.sort(key=lambda m: (REL_ORDER.get(m.role, 9), m.patient_id))
    return by_case


def _query_reason(member: CaseMember, excluded_tenants: list[str]) -> str | None:
    """Why this member cannot run, from its facts: most specific first."""
    if member.seq_id is None:
        return "pending_sequencing"
    if member.current_alignment_task_id is None:
        return "pending_alignment"
    if member.gvcf_matches == 0:
        return "no_gvcf"
    if member.gvcf_matches > 1:
        return "ambiguous_gvcf"
    if member.project_code is None:
        return "no_project_code"
    if member.tenant_code in excluded_tenants:
        return "tenant_excluded"
    return None


def _validate(case_id: int, members: list[CaseMember]) -> list[tuple[Reason, str]]:
    """`(reason, message)` pairs. Exclusion is at case granularity: the pipeline joint-calls
    whole families, so a family missing a member is not a smaller job, it is a different one.
    """
    problems: list[tuple[Reason, str]] = []

    # Reasons the query already worked out. It sees the supersession rules; this does not.
    for member in members:
        if member.exclusion_reason:
            reason = from_query(member.exclusion_reason).about(member.patient_id)
            problems.append((reason, f"case {case_id}, patient {member.patient_id}: {reason.text}"))

    # The aliquot is every id the pipeline sees (samplesheet, PED, phenopacket) because it is
    # the gVCF sample name. The query only leaves it null on a member it has already excluded
    # as `pending_sequencing`; this names the failure should that ever stop being true.
    for member in members:
        if not member.exclusion_reason and not member.aliquot:
            message = f"case {case_id}, patient {member.patient_id}: its sequencing experiment has no aliquot"
            problems.append((Reason(code=ReasonCode.MISSING_ALIQUOT, patient_id=member.patient_id), message))

    probands = [m for m in members if m.role == "proband"]
    if len(probands) != 1:
        # Now means what it says. Before one experiment per member was selected, a
        # re-sequenced proband produced two rows and tripped this on a cause it never named.
        problems.append(
            (
                Reason(code=ReasonCode.PROBAND_COUNT),
                f"case {case_id}: expected exactly 1 proband, found {len(probands)} -- two "
                f"different patients are marked proband on this case",
            )
        )

    strategies = {m.strategy for m in members if m.strategy}
    unknown = {s for s in strategies if s.lower() not in SEQUENCING_TYPES}
    if unknown:
        problems.append(
            (
                Reason(code=ReasonCode.UNSUPPORTED_STRATEGY, detail=",".join(sorted(unknown))),
                f"case {case_id}: unsupported experimental strategy {sorted(unknown)}",
            )
        )
    elif len(strategies) > 1:
        problems.append(
            (
                Reason(code=ReasonCode.UNSUPPORTED_STRATEGY, detail=",".join(sorted(strategies))),
                f"case {case_id}: members span several strategies {sorted(strategies)}",
            )
        )

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
