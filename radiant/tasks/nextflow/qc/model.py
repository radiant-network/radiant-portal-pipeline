"""Member and case models for the case-driven quality-control DAG."""

from pydantic import BaseModel

# `familyId` is `CA<case id>`, exactly as for post-processing: the pipeline names its
# per-family MultiQC output after it, so a report maps back to a case by arithmetic.
from radiant.tasks.nextflow.model import case_id_of, family_id  # noqa: F401  (re-exported)


class QcMember(BaseModel):
    """One family member: one row of `sql/clinical/pending_quality_control_select.sql`.

    A member with no alignment comes back with null document fields and an
    `exclusion_reason`.
    """

    case_id: int
    submitter_case_id: str
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
    alignment_task_id: int | None = None
    cram_url: str | None = None
    cram_matches: int = 0
    crai_url: str | None = None
    # The alignment's `ssup`/`tgz` output. None means the samplesheet falls back to the
    # CRAM's directory.
    dragen_metrics_url: str | None = None
    dragen_metrics_matches: int = 0
    exclusion_reason: str | None = None


class QcCase(BaseModel):
    """One germline case, resolved and validated: one `familyId`, one QC task."""

    case_id: int
    family_id: str
    submitter_case_id: str
    tenant_code: str
    project_code: str | None = None
    # Proband first, then father, mother, then the rest.
    members: list[QcMember]

    @property
    def proband(self) -> QcMember:
        return self.members[0]
