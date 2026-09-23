"""Build the `PATCH /{tenant}/cases/batch` body registering a CNV post-processing run.

Two tasks per case, mirroring the annotation DAG:

- `radiant_germline_cnv_annotation`, bound to every member's aliquot, with the per-member
  germline CNV VCFs (and the CRAM/index pairs, when the run used them) as inputs and the
  slivar VCF set as outputs;
- `exomiser_cnv`, single-aliquot (the proband), with the slivar VCF as its in-batch input
  and the Exomiser reports as outputs.

`exomiser_cnv` rather than `exomiser`: the ETL ingests every `variants.tsv` published by an
`exomiser` task into the SNV Exomiser table, and a CNV report there would be wrong data, not
merely surplus. A distinct type keeps it out.

Same four backend rules as `radiant.tasks.nextflow.batch`: inputs mandatory (TASK-003),
resolvable in-tenant or in-batch (TASK-005), single-aliquot Exomiser (TASK-007). PATCH
appends: a re-run adds a second pair of tasks alongside the first.
"""

from radiant.tasks.nextflow.batch import EXOMISER_PIPELINE, GENOME_BUILD
from radiant.tasks.nextflow.cnv.model import CnvFamily
from radiant.tasks.nextflow.cnv.outputs import ANNOTATION_OUTPUTS, EXOMISER_OUTPUTS

ANNOTATION_TASK_TYPE = "radiant_germline_cnv_annotation"
EXOMISER_TASK_TYPE = "exomiser_cnv"

# The revision the launcher image pins (`CNV_PIPELINE_REV` in Dockerfile.nextflow.launcher).
# A commit of feat/BIOINFO-214-expand-CNV-post-processing until the pipeline is tagged.
# Bump both together.
ANNOTATION_PIPELINE = ("cnv-post-processing", "6b9b2dd")


def build_patch_body(families: list[CnvFamily], collected: dict[str, dict]) -> dict:
    """`families` from resolve_cases, `collected` from collect_outputs (keyed by family id)."""
    cases = []
    for family in families:
        documents = collected[family.family_id]
        slivar_vcf_url = documents["slivar_vcf"]["url"]

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
                        "output_documents": [documents[name] for name in ANNOTATION_OUTPUTS],
                    },
                    {
                        "type_code": EXOMISER_TASK_TYPE,
                        "aliquots": [family.proband.aliquot],
                        "pipeline_name": EXOMISER_PIPELINE[0],
                        "pipeline_version": EXOMISER_PIPELINE[1],
                        "genome_build": GENOME_BUILD,
                        # Exomiser actually reads the depth-refined (or VEP-annotated) family
                        # VCF, one or two steps before slivar; neither is published as a
                        # document, so naming one would fail TASK-005. Recording the slivar
                        # VCF is the same deliberate compromise the annotation DAG makes.
                        "input_documents": [{"url": slivar_vcf_url}],
                        "output_documents": [documents[name] for name in EXOMISER_OUTPUTS],
                    },
                ],
            }
        )
    return {"cases": cases}
