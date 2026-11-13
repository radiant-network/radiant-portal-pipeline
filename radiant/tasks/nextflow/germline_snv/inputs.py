"""Build the pipeline's three input artefacts: samplesheet, PED, phenopacket.

Ported from the local prototype that produced the 1kGP run
(`tmp/nextflow/onekg-inputs/build_nextflow_inputs.py`), with two changes: `familyId` is
now `CA<case id>` (see `model.family_id`), and the files are returned as strings for the
caller to put on S3 rather than written to the local filesystem.

The samplesheet references the PED and phenopacket by **pod path**, not S3 URI: the
pipeline reads them off the FSx mount. Writing them to S3 is enough for them to appear
there, which is what keeps `generate_inputs` a plain Airflow task instead of a pod with a
PVC.

Every individual id handed to the pipeline -- the samplesheet's `sample`, the PED ids, the
phenopacket's subject and pedigree ids -- is the **aliquot**, not the submitter sample id.
DRAGEN names the gVCF sample column after the aliquot, and Exomiser refuses a phenopacket
whose ids are not found among the VCF samples. The 1kGP prototype hid this: there the two
ids are the same string.
"""

import csv
import io

from radiant.tasks.nextflow.family import PED_DIR, PHENO_DIR, build_ped, build_phenopacket
from radiant.tasks.nextflow.germline_snv.model import SnvFamily
from radiant.tasks.nextflow.paths import to_mount

# Fixed by the pipeline's nf-schema definition.
SAMPLESHEET_COLUMNS = ["familyId", "sample", "sequencingType", "gvcf", "familyPheno", "familyPed"]


def build_inputs(
    families: list[SnvFamily], input_prefix_pod: str, inputs_root: str, inputs_mount: str
) -> dict[str, str]:
    """Return `{relative key: file content}` for the whole run.

    Keys are relative to the run's input prefix, so the caller only has to prepend a
    bucket and a prefix to write them, and the same relative layout appears on the mount.
    """
    files = {"samplesheet.csv": build_samplesheet(families, input_prefix_pod, inputs_root, inputs_mount)}
    for family in families:
        files[f"{PED_DIR}/{family.family_id}.ped"] = build_ped(family)
        files[f"{PHENO_DIR}/{family.family_id}.yml"] = build_phenopacket(family)
    return files


def build_samplesheet(families: list[SnvFamily], input_prefix_pod: str, inputs_root: str, inputs_mount: str) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SAMPLESHEET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for family in families:
        for member in family.members:
            writer.writerow(
                {
                    "familyId": family.family_id,
                    "sample": member.aliquot,
                    "sequencingType": family.sequencing_type,
                    "gvcf": to_mount(member.gvcf_url, inputs_root, inputs_mount),
                    "familyPheno": f"{input_prefix_pod}/{PHENO_DIR}/{family.family_id}.yml",
                    "familyPed": f"{input_prefix_pod}/{PED_DIR}/{family.family_id}.ped",
                }
            )
    return buffer.getvalue()
