"""Row, member and case models for the case-driven quality-control DAG."""

from pydantic import BaseModel

# `familyId` is `CA<case id>`, exactly as for post-processing: the pipeline names its
# per-family MultiQC output after it, so a report maps back to a case by arithmetic.
from radiant.tasks.nextflow.model import case_id_of, family_id  # noqa: F401  (re-exported)


class DocumentRow(BaseModel):
    """One row of `sql/clinical/pending_quality_control_select.sql`.

    A member comes back once per output document of its current alignment -- the DRAGEN
    metrics are not documents, and are found by probing next to whichever of those documents
    they were written beside. A member with no alignment comes back once, with null document
    fields and an `exclusion_reason`.
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
    document_url: str | None = None
    document_data_type: str | None = None
    document_format: str | None = None
    exclusion_reason: str | None = None


class QcMember(BaseModel):
    """One family member, with its document rows folded into `document_urls`."""

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
    # Every output url of the current alignment, sorted. The candidate directories for the
    # DRAGEN metrics are the parents of these.
    document_urls: list[str] = []
    # Where this member's `<aliquot>.mapping_metrics.csv` was actually found. Set by the probe.
    metrics_dir_s3: str | None = None
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
    # The one directory the launcher run covering this case is pointed at. Set by the probe;
    # a case whose members' metrics sit in different directories is excluded instead.
    metrics_dir_s3: str | None = None

    @property
    def proband(self) -> QcMember:
        return self.members[0]


class MetricsGroup(BaseModel):
    """The cases one launcher run covers: those whose metrics share a directory.

    `--dragen_metrics_dir` is a single directory per Nextflow run, so this is the unit of a
    child run, not the case. Each group gets its own run tag, and so its own input prefix,
    launch directory and outdir.
    """

    index: int
    run_tag: str
    metrics_dir_s3: str
    case_ids: list[int]
