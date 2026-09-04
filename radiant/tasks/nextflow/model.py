"""Row and family models for the case-driven post-processing DAG."""

from pydantic import BaseModel

# `familyId` is `CA` + `cases.id` -- CA1072, not 1072 and not submitter_case_id.
#
# The prefix is not cosmetic. nf-schema types familyId as a string, but its CSV reader
# coerces a bare numeric value to a number first, and the property then fails validation
# with its single errorMessage: "familyId must be provided and cannot contain spaces".
# Quoting the field does not help; a non-numeric prefix does.
#
# Deriving it from cases.id rather than submitter_case_id also makes it stable and
# collision-free -- a submitter id is free text, can change, and carries no uniqueness
# guarantee -- and makes going from an output filename back to a case arithmetic.
FAMILY_ID_PREFIX = "CA"


def family_id(case_id: int) -> str:
    return f"{FAMILY_ID_PREFIX}{case_id}"


def case_id_of(fam_id: str) -> int:
    """Inverse of `family_id`, for reading a case back out of an output filename."""
    if not fam_id.startswith(FAMILY_ID_PREFIX):
        raise ValueError(f"not a family id: {fam_id!r}")
    return int(fam_id[len(FAMILY_ID_PREFIX) :])


class CaseMember(BaseModel):
    """One row of `sql/clinical/pending_annotation_select.sql`.

    Everything describing the member's sequencing is optional, because the query returns a
    member that has none rather than dropping it. A family silently short one person is a
    worse failure than a family the run refuses to build, so the row comes back carrying
    `exclusion_reason` and the case is excluded whole.
    """

    case_id: int
    submitter_case_id: str
    primary_condition: str | None = None
    tenant_code: str
    project_code: str | None = None
    role: str
    affected_status: str
    patient_id: int
    sex: str
    submitter_patient_id: str | None = None
    sample_id: str | None = None
    seq_id: int | None = None
    aliquot: str | None = None
    strategy: str | None = None
    # The alignment task the gVCF hangs off -- the newest one for this experiment. Carried
    # so a targeted `task_ids` run can report a task that produced no candidate case.
    alignment_task_id: int | None = None
    gvcf_url: str | None = None
    gvcf_matches: int = 0
    # Set by the query when this member is why the case cannot run. None means usable.
    exclusion_reason: str | None = None


class Phenotype(BaseModel):
    """One row of `sql/clinical/case_phenotypes_select.sql`."""

    case_id: int
    patient_id: int
    hpo_id: str
    hpo_label: str | None = None
    onset_code: str | None = None
    interpretation_code: str | None = None

    @property
    def observed(self) -> bool:
        """`negative` means the term was explicitly excluded for this patient."""
        return self.interpretation_code == "positive"


class Family(BaseModel):
    """One germline case, resolved and validated: the unit the pipeline runs on."""

    case_id: int
    family_id: str
    submitter_case_id: str
    # Read off the case, not asked for: `cases.id` is globally unique, so it already names
    # the tenant. This is what the batch PATCH is addressed to.
    tenant_code: str
    project_code: str | None = None
    sequencing_type: str
    # Proband first, then father, mother, then the rest -- the order the PED and
    # phenopacket writers assume.
    members: list[CaseMember]
    # The proband's terms only: Exomiser ranks the proband, and the pipeline's
    # phenopacket carries one `proband` block.
    phenotypes: list[Phenotype] = []

    @property
    def proband(self) -> CaseMember:
        return self.members[0]

    @property
    def father(self) -> CaseMember | None:
        return next((m for m in self.members if m.role == "father"), None)

    @property
    def mother(self) -> CaseMember | None:
        return next((m for m in self.members if m.role == "mother"), None)
