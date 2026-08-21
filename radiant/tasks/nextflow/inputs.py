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
import json

from radiant.tasks.nextflow.model import Family
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
    "iriPrefix": "'http://purl.obolibrary.org/obo/HP_'",
}


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
    proband = family.proband
    out = [
        "---",
        f"id: {yaml_str(family.family_id)}",
        "proband:",
        "  subject:",
        f"    id: {yaml_str(proband.sample_id)}",
        f"    sex: {PPKT_SEX[proband.sex]}",
    ]

    if family.phenotypes:
        out.append("  phenotypicFeatures:")
        # Observed terms first: Exomiser ranks on them, excluded ones only penalise.
        for pheno in sorted(family.phenotypes, key=lambda p: (not p.observed, p.hpo_id)):
            out += [
                "    - type:",
                f"        id: {yaml_str(pheno.hpo_id)}",
                f"        label: {yaml_str(pheno.hpo_label or pheno.hpo_id)}",
            ]
            if not pheno.observed:
                out.append("      excluded: true")

    out += ["", "pedigree:", "  persons:"]
    for member in family.members:
        out.append(f"    - individualId: {yaml_str(member.sample_id)}")
        if member.role == "proband":
            if family.father:
                out.append(f"      paternalId: {yaml_str(family.father.sample_id)}")
            if family.mother:
                out.append(f"      maternalId: {yaml_str(family.mother.sample_id)}")
        out += [
            f"      sex: {PPKT_SEX[member.sex]}",
            f"      affectedStatus: {PPKT_AFFECTED[member.affected_status]}",
        ]

    out += ["", "metaData:", "  resources:"]
    for index, (key, value) in enumerate(HPO_RESOURCE.items()):
        out.append(f"    {'- ' if index == 0 else '  '}{key}: {yaml_str(value)}")
    out.append("  phenopacketSchemaVersion: 2.0")

    return "\n".join(out) + "\n"


def yaml_str(value: str) -> str:
    """Quote only what has to be quoted, so the output stays readable."""
    if value.startswith("'") and value.endswith("'"):
        return value  # already quoted in the constant above
    needs_quotes = (
        value != value.strip()
        or value == ""
        or value[0] in "-?:,[]{}#&*!|>'\"%@`"
        or ": " in value
        or value.endswith(":")
        or " #" in value
        # An all-digit id would load as an int, and phenopacket ids are string fields.
        or _looks_numeric(value)
    )
    return json.dumps(value) if needs_quotes else value


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return value.lower() in {"true", "false", "null", "yes", "no", "on", "off", "~"}
    return True
