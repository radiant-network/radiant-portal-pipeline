"""The germline family: its ids, its members, its phenotypes, and the PED and phenopacket files
built from them. Shared by the family pipelines (`germline_snv/`, `germline_cnv/`); `qc/` reuses
the family ids.

Every individual id in the PED and the phenopacket is the **aliquot**, not the submitter sample
id: DRAGEN names the VCF sample column after the aliquot, and Exomiser refuses a phenopacket whose
ids are not found among the VCF samples.
"""

import yaml
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


class FamilyMember(BaseModel):
    """One member of a germline case, as the fact and pending queries return it.

    Everything describing the member's sequencing is optional, because the queries return a
    member that has none rather than dropping it: a family silently short one person is a worse
    failure than a family the run refuses to build.
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
    # The current alignment task of this experiment, where the documents hang.
    alignment_task_id: int | None = None
    # The case already has this pipeline's annotation for this experiment.
    annotated: bool = False


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
    """One germline case, resolved and validated: the unit a family pipeline runs on."""

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
    members: list[FamilyMember]
    # The proband's terms only: Exomiser ranks the proband, and the pipeline's
    # phenopacket carries one `proband` block.
    phenotypes: list[Phenotype] = []

    @property
    def proband(self) -> FamilyMember:
        return self.members[0]

    @property
    def father(self) -> FamilyMember | None:
        return next((m for m in self.members if m.role == "father"), None)

    @property
    def mother(self) -> FamilyMember | None:
        return next((m for m in self.members if m.role == "mother"), None)


PED_DIR = "pedigrees"
PHENO_DIR = "phenotypes"

# The children of the case's father and mother. A fetus is the proband's child, not theirs (RAD-95).
CHILD_ROLES = ("proband", "brother", "sister", "sibling")

PED_SEX = {"male": "1", "female": "2", "unknown": "0"}
PED_AFFECTED = {"affected": "2", "non_affected": "1", "unknown": "0"}
PPKT_SEX = {"male": "MALE", "female": "FEMALE", "unknown": "UNKNOWN_SEX"}
PPKT_AFFECTED = {"affected": "AFFECTED", "non_affected": "UNAFFECTED", "unknown": "MISSING"}

HPO_RESOURCE = {
    "id": "hp",
    "name": "human phenotype ontology",
    "url": "http://purl.obolibrary.org/obo/hp.owl",
    "version": "hp/releases/2019-11-08",
    "namespacePrefix": "HP",
    "iriPrefix": "http://purl.obolibrary.org/obo/HP_",
}

PHENOPACKET_SCHEMA_VERSION = 2.0


class _Dumper(yaml.SafeDumper):
    """Indent sequences under their parent key, as the reference phenopackets do.

    PyYAML puts `- ` at the parent's own indent level, which parses identically but reads
    nothing like the files this format is usually seen in.
    """

    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def build_ped(family: Family) -> str:
    father, mother = family.father, family.mother
    lines = []
    for member in family.members:
        # Every child is linked, not only the proband: slivar gives parental origin and trio tags to each child
        # with a parent in the family (RAD-95).
        is_child = member.role in CHILD_ROLES
        lines.append(
            "\t".join(
                [
                    family.family_id,
                    member.aliquot,
                    father.aliquot if (is_child and father) else "0",
                    mother.aliquot if (is_child and mother) else "0",
                    PED_SEX[member.sex],
                    PED_AFFECTED[member.affected_status],
                ]
            )
        )
    return "\n".join(lines) + "\n"


def build_phenopacket(family: Family) -> str:
    """The Exomiser phenopacket for one family, as YAML.

    Only the proband carries phenotypic features: Exomiser ranks the proband, and the
    format has a single `proband` block. The pedigree still lists every member.
    """
    proband = family.proband
    proband_block = {"subject": {"id": proband.aliquot, "sex": PPKT_SEX[proband.sex]}}

    if family.phenotypes:
        # Observed terms first: Exomiser ranks on them, excluded ones only penalise.
        proband_block["phenotypicFeatures"] = [
            _feature(pheno) for pheno in sorted(family.phenotypes, key=lambda p: (not p.observed, p.hpo_id))
        ]

    document = {
        "id": family.family_id,
        "proband": proband_block,
        "pedigree": {"persons": [_person(family, member) for member in family.members]},
        "metaData": {
            "resources": [HPO_RESOURCE],
            "phenopacketSchemaVersion": PHENOPACKET_SCHEMA_VERSION,
        },
    }
    return yaml.dump(
        document,
        Dumper=_Dumper,
        # Key order is meaning here, not style: `id` then `proband` then `pedigree` reads
        # as the format's own shape, and alphabetising it would not.
        sort_keys=False,
        explicit_start=True,
        default_flow_style=False,
        allow_unicode=True,
        # HPO labels and the ontology URLs are long; folding them across lines parses fine
        # but makes the file unreadable next to the reference phenopackets.
        width=4096,
    )


def _feature(pheno: Phenotype) -> dict:
    feature = {"type": {"id": pheno.hpo_id, "label": pheno.hpo_label or pheno.hpo_id}}
    if not pheno.observed:
        feature["excluded"] = True
    return feature


def _person(family: Family, member: FamilyMember) -> dict:
    person = {"individualId": member.aliquot}
    if member.role == "proband":
        if family.father:
            person["paternalId"] = family.father.aliquot
        if family.mother:
            person["maternalId"] = family.mother.aliquot
    person["sex"] = PPKT_SEX[member.sex]
    person["affectedStatus"] = PPKT_AFFECTED[member.affected_status]
    return person
