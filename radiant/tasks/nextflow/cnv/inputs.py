"""Build the CNV pipeline's three input artefacts: samplesheet, PED, phenopacket.

The PED and phenopacket are the post-processing ones, unchanged: `CnvFamily` is a `Family`,
so `build_ped` and `build_phenopacket` apply as they are. Only the samplesheet is
pipeline-shaped.

Every case takes the pipeline's *family* route, singletons included, because every row
carries `familyPed` and `familyPheno`. That is what keeps the output names keyed by
`familyId` alone (`slivar/CA<id>.cnv.slivar.vcf.gz`, `exomiser/CA<id>.exomiser.*`); the
per-sample `pheno` column, which selects the pipeline's solo route, is deliberately left
empty.

File paths are **pod paths**, not S3 URIs: the pipeline stats every one of them at launch
(`exists: true` in its schema), so a document registered outside the workspace bucket fails
here, in `to_mount`, rather than in the driver pod.
"""

import csv
import io

from radiant.tasks.nextflow.cnv.model import CnvFamily
from radiant.tasks.nextflow.inputs import PED_DIR, PHENO_DIR, build_ped, build_phenopacket
from radiant.tasks.nextflow.paths import to_mount

__all__ = ["SAMPLESHEET_COLUMNS", "CALLER", "build_inputs", "build_samplesheet", "build_ped", "build_phenopacket"]

# Fixed by the pipeline's `assets/schema_input.json`.
SAMPLESHEET_COLUMNS = [
    "familyId",
    "sample",
    "sequencingType",
    "caller",
    "vcf",
    "cram",
    "pheno",
    "familyPheno",
    "familyPed",
]

# The only per-sample caller the pipeline's normalization step implements. `resolve` has
# already excluded any case whose alignment was not produced by DRAGEN.
CALLER = "DRAGEN"


def build_inputs(
    families: list[CnvFamily], input_prefix_pod: str, inputs_root: str, inputs_mount: str
) -> dict[str, str]:
    """Return `{relative key: file content}` for the whole run, as post-processing does."""
    files = {"samplesheet.csv": build_samplesheet(families, input_prefix_pod, inputs_root, inputs_mount)}
    for family in families:
        files[f"{PED_DIR}/{family.family_id}.ped"] = build_ped(family)
        files[f"{PHENO_DIR}/{family.family_id}.yml"] = build_phenopacket(family)
    return files


def build_samplesheet(families: list[CnvFamily], input_prefix_pod: str, inputs_root: str, inputs_mount: str) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SAMPLESHEET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for family in families:
        # All-or-nothing per family: see `CnvFamily.crams_complete`.
        with_crams = family.crams_complete
        for member in family.members:
            writer.writerow(
                {
                    "familyId": family.family_id,
                    "sample": member.aliquot,
                    "sequencingType": family.sequencing_type,
                    "caller": CALLER,
                    "vcf": to_mount(member.gcnv_url, inputs_root, inputs_mount),
                    "cram": to_mount(member.cram_url, inputs_root, inputs_mount) if with_crams else "",
                    "pheno": "",
                    "familyPheno": f"{input_prefix_pod}/{PHENO_DIR}/{family.family_id}.yml",
                    "familyPed": f"{input_prefix_pod}/{PED_DIR}/{family.family_id}.ped",
                }
            )
    return buffer.getvalue()
