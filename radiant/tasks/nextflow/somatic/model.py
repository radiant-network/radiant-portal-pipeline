"""Row and task models for the somatic SNV annotation DAG."""

from pydantic import BaseModel

CALLING_TASK_TYPES = ("somatic_variant_calling", "tumor_only_variant_calling", "alignment_somatic_variant_calling")

TUMORAL = "tumoral"
NORMAL = "normal"


class CallingTaskRow(BaseModel):
    """One row of `sql/clinical/pending_somatic_annotation_select.sql`: one experiment of one
    calling task, framed by one somatic case.

    Everything describing the experiment or the VCF is optional: the query returns a task that
    cannot run with its `exclusion_reason` rather than dropping it.
    """

    case_id: int
    submitter_case_id: str
    tenant_code: str
    project_code: str | None = None
    calling_task_id: int
    calling_task_type: str
    seq_id: int
    aliquot: str | None = None
    strategy: str | None = None
    histology: str | None = None
    ssnv_url: str | None = None
    ssnv_matches: int = 0
    # Set by the query when this row is why the task, or this case of it, cannot run.
    exclusion_reason: str | None = None


class SomaticCase(BaseModel):
    """A case the annotation is registered on. The batch PATCH looks it up by
    `(project_code, submitter_case_id)`, in `tenant_code`."""

    case_id: int
    submitter_case_id: str
    tenant_code: str
    project_code: str


class CallingTask(BaseModel):
    """One calling task, resolved and validated: the unit the pipeline runs on."""

    task_id: int
    task_type: str
    sequencing_type: str
    # The raw `ssnv` VCF the calling task published: the samplesheet's `vcf`, and the annotation
    # task's input document.
    vcf_url: str
    tumor_aliquot: str
    normal_aliquot: str | None = None
    # Usually one. A calling task whose experiments several somatic cases hold is annotated once
    # and registered on each of them.
    cases: list[SomaticCase]

    @property
    def family_id(self) -> str:
        """The samplesheet's `familyId`: the calling task id. The pipeline names its output
        `ensemblvep/variants.<familyId>.snv.vep.vcf.gz` after it, so a file maps back to its task
        by arithmetic. The pinned pipeline revision types `familyId` as string or integer, so no
        prefix is needed, unlike `CA<case id>` was for v3.0.0."""
        return str(self.task_id)

    @property
    def aliquots(self) -> list[str]:
        """The calling task's own aliquots, tumor first. The portal derives `task_context` from
        them, so the TO/TN classification of the annotation is the calling task's."""
        return [self.tumor_aliquot, *([self.normal_aliquot] if self.normal_aliquot else [])]

    @property
    def tenants(self) -> list[str]:
        return sorted({c.tenant_code for c in self.cases})
