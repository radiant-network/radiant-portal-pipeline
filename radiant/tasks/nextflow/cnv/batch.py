"""Build the `PATCH /{tenant}/cases/batch` body registering a CNV post-processing run.

One task per case, mirroring the annotation DAG (RAD-116): `radiant_germline_cnv_annotation`,
bound to every member's aliquot, with the per-member germline CNV VCFs (and the CRAM/index
pairs, when the run used them) as inputs, and the slivar VCF set and the Exomiser reports as
outputs.

The Exomiser reports are typed `exomiser_cnv`, not `exomiser`, so users can tell them from the
SNV report. The ETL reads no output of this task, and its staging view admits only the
`exomiser` data type: the CNV report stays out of the SNV Exomiser table.

Same backend rules as `radiant.tasks.nextflow.batch`: inputs mandatory (TASK-003), resolvable
in-tenant (TASK-005). PATCH appends: a re-run adds a second task alongside the first.
"""

from radiant.tasks.nextflow.batch import GENOME_BUILD
from radiant.tasks.nextflow.cnv.model import CnvFamily
from radiant.tasks.nextflow.cnv.outputs import ANNOTATION_OUTPUTS, EXOMISER_OUTPUTS

ANNOTATION_TASK_TYPE = "radiant_germline_cnv_annotation"

# The revision the launcher image pins (`CNV_PIPELINE_REV` in Dockerfile.nextflow.launcher).
# A commit of feat/BIOINFO-214-expand-CNV-post-processing until the pipeline is tagged.
# Bump both together.
ANNOTATION_PIPELINE = ("cnv-post-processing", "cad99b4")


def build_patch_body(families: list[CnvFamily], collected: dict[str, dict]) -> dict:
    """`families` from resolve_cases, `collected` from collect_outputs (keyed by family id)."""
    cases = []
    for family in families:
        documents = collected[family.family_id]

        # The same files the samplesheet fed to the pipeline: every member's CNV VCF, plus
        # the CRAM and its index when the run refined genotypes from them.
        inputs = [{"url": m.gcnv_url} for m in family.members]
        if family.crams_complete:
            inputs += [{"url": url} for m in family.members for url in (m.cram_url, m.crai_url)]

        cases.append(
            {
                "project_code": family.project_code,
                "submitter_case_id": family.submitter_case_id,
                "tasks": [
                    {
                        "type_code": ANNOTATION_TASK_TYPE,
                        "aliquots": [m.aliquot for m in family.members],
                        "pipeline_name": ANNOTATION_PIPELINE[0],
                        "pipeline_version": ANNOTATION_PIPELINE[1],
                        "genome_build": GENOME_BUILD,
                        "input_documents": inputs,
                        "output_documents": [documents[name] for name in ANNOTATION_OUTPUTS + EXOMISER_OUTPUTS],
                    },
                ],
            }
        )
    return {"cases": cases}
