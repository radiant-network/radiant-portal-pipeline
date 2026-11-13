"""Row and family models for germline SNV post-processing."""

from radiant.tasks.nextflow.family import Family, FamilyMember


class SnvMember(FamilyMember):
    """One row of `sql/clinical/germline_snv_facts_select.sql`: a member with its gVCF facts."""

    gvcf_url: str | None = None
    gvcf_matches: int = 0


class SnvFamily(Family):
    """One germline case, resolved and validated: the unit SNV post-processing runs on."""

    members: list[SnvMember]
