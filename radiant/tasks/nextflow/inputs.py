"""Build the pipeline's three input artefacts: samplesheet, PED, phenopacket.

Ported from the local prototype that produced the 1kGP run
(`tmp/nextflow/onekg-inputs/build_nextflow_inputs.py`), with two changes: `familyId` is
now `CA<case id>` (see `model.family_id`), and the files are returned as strings for the
caller to put on S3 rather than written to the local filesystem.

The samplesheet references the PED and phenopacket by **pod path**, not S3 URI: the
pipeline reads them off the FSx mount. Writing them to S3 is enough for them to appear
there, which is what keeps `generate_inputs` a plain Airflow task instead of a pod with a
PVC.
"""

import csv
import io

import yaml

from radiant.tasks.nextflow.model import CaseMember, Family, Phenotype
from radiant.tasks.nextflow.paths import to_mount

# Fixed by the pipeline's nf-schema definition.
SAMPLESHEET_COLUMNS = ["familyId", "sample", "sequencingType", "gvcf", "familyPheno", "familyPed"]

PED_DIR = "pedigrees"
PHENO_DIR = "phenotypes"

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


def build_inputs(families: list[Family], input_prefix_pod: str, inputs_root: str, inputs_mount: str) -> dict[str, str]:
    """Return `{relative key: file content}` for the whole run.

    Keys are relative to the run's input prefix, so the caller only has to prepend a
    bucket and a prefix to write them, and the same relative layout appears on the mount.
    """
    files = {"samplesheet.csv": build_samplesheet(families, input_prefix_pod, inputs_root, inputs_mount)}
    for family in families:
        files[f"{PED_DIR}/{family.family_id}.ped"] = build_ped(family)
        files[f"{PHENO_DIR}/{family.family_id}.yml"] = build_phenopacket(family)
    return files


def build_samplesheet(families: list[Family], input_prefix_pod: str, inputs_root: str, inputs_mount: str) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SAMPLESHEET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for family in families:
        for member in family.members:
            writer.writerow(
                {
                    "familyId": family.family_id,
                    "sample": member.sample_id,
                    "sequencingType": family.sequencing_type,
                    "gvcf": to_mount(member.gvcf_url, inputs_root, inputs_mount),
                    "familyPheno": f"{input_prefix_pod}/{PHENO_DIR}/{family.family_id}.yml",
                    "familyPed": f"{input_prefix_pod}/{PED_DIR}/{family.family_id}.ped",
                }
            )
    return buffer.getvalue()


def build_ped(family: Family) -> str:
    father, mother = family.father, family.mother
    lines = []
    for member in family.members:
        is_proband = member.role == "proband"
        lines.append(
            "\t".join(
                [
                    family.family_id,
                    member.sample_id,
                    father.sample_id if (is_proband and father) else "0",
                    mother.sample_id if (is_proband and mother) else "0",
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
    proband_block = {"subject": {"id": proband.sample_id, "sex": PPKT_SEX[proband.sex]}}

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


def _person(family: Family, member: CaseMember) -> dict:
    person = {"individualId": member.sample_id}
    if member.role == "proband":
        if family.father:
            person["paternalId"] = family.father.sample_id
        if family.mother:
            person["maternalId"] = family.mother.sample_id
    person["sex"] = PPKT_SEX[member.sex]
    person["affectedStatus"] = PPKT_AFFECTED[member.affected_status]
    return person
