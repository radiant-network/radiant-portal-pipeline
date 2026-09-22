"""Row and family models for the case-driven CNV post-processing DAG."""

# `familyId` is `CA<case id>`, exactly as for post-processing: the pipeline names its
# family-level outputs after it, so a file maps back to a case by arithmetic.
from radiant.tasks.nextflow.model import CaseMember, Family, case_id_of, family_id  # noqa: F401  (re-exported)


class CnvMember(CaseMember):
    """One row of `sql/clinical/pending_cnv_annotation_select.sql`.

    Extends the annotation row with the CNV trigger document and the optional alignment.
    The inherited `gvcf_url` / `gvcf_matches` are not populated by this query and stay at
    their defaults; subclassing is what lets the PED and phenopacket writers, and
    `Family.proband/father/mother`, be reused unchanged.
    """

    # The caller that produced the alignment (`task.pipeline_name`). The pipeline's
    # normalization step only implements DRAGEN's CNV conventions and fails on anything else.
    alignment_pipeline: str | None = None
    gcnv_url: str | None = None
    gcnv_matches: int = 0
    # Optional: feeds the depth-based genotype refinement. The pipeline resolves the index
    # as `<cram>.crai` by *file existence on the mount*, so the crai must sit at that path.
    cram_url: str | None = None
    crai_url: str | None = None


class CnvFamily(Family):
    """One germline case, resolved and validated: the unit the CNV pipeline runs on."""

    members: list[CnvMember]

    @property
    def crams_complete(self) -> bool:
        """Every member has a CRAM whose index is registered at `<cram>.crai`.

        All-or-nothing per family, on purpose: the pipeline refines a family only when
        every sample has an alignment, and warns and skips otherwise -- but a CRAM whose
        index is *not* at `<cram>.crai` makes mosdepth fail rather than skip. Passing
        CRAMs only when the whole set is usable turns both cases into a clean skip.
        """
        return all(m.cram_url and m.crai_url == f"{m.cram_url}.crai" for m in self.members)
