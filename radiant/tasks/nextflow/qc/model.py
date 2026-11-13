"""Member and case models for the case-driven quality-control DAG."""

from pydantic import BaseModel

SOMATIC = "somatic"


class QcMember(BaseModel):
    """One member of a case: one row of `sql/clinical/pending_quality_control_select.sql` or its somatic twin.

    A member with no alignment comes back with null document fields and an
    `exclusion_reason`. A somatic case has one member, its proband's tumor sample.
    """

    case_id: int
    submitter_case_id: str
    tenant_code: str
    case_type: str
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
    alignment_task_id: int | None = None
    # None for a somatic member: its CRAM selects and excludes nothing.
    cram_url: str | None = None
    cram_matches: int = 0
    crai_url: str | None = None
    # The alignment's `ssup`/`tgz` output. None means the samplesheet falls back to the
    # CRAM's directory; a somatic member without one is excluded as `no_metrics`.
    dragen_metrics_url: str | None = None
    dragen_metrics_matches: int = 0
    exclusion_reason: str | None = None


class QcCase(BaseModel):
    """One case, resolved and validated: one `familyId`, one QC task."""

    case_id: int
    family_id: str
    submitter_case_id: str
    tenant_code: str
    case_type: str
    project_code: str | None = None
    # Proband first, then father, mother, then the rest.
    members: list[QcMember]

    @property
    def proband(self) -> QcMember:
        return self.members[0]

    @property
    def is_somatic(self) -> bool:
        return self.case_type == SOMATIC
