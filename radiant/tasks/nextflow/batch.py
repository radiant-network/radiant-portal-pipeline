"""Build the `PATCH /{tenant}/cases/batch` body registering a run's outputs.

PATCH, not POST: the cases already exist, are looked up by
`(project_code, submitter_case_id)`, and array fields **append** -- so the existing
`alignment_germline_variant_calling` tasks survive, and re-running the same cases adds a
second annotation + exomiser task alongside the first rather than replacing it. That is
the platform's own model: `staging_sequencing_experiment` keys on
`(case_id, seq_id, task_id)` and the portal serves `tasks_with_occurrences` (plural), so a
user can pick between them.

Four backend rules, any of which fails the whole batch:

- `input_documents` is **mandatory** for both task types (they are in
  `RequiresInputDocumentsTaskTypes`); omitting it is TASK-003.
- inputs must resolve to a document already in the tenant, or to an output of another task
  in the same batch (TASK-005).
- `exomiser` is single-aliquot (`SingleAliquotTaskTypes`); more than one is TASK-007.
- the ETL reads only two shapes -- see `outputs.OUTPUT_SPEC`.
"""

from radiant.tasks.nextflow.model import Family
from radiant.tasks.nextflow.outputs import ANNOTATION_OUTPUTS, EXOMISER_OUTPUTS

ANNOTATION_TASK_TYPE = "radiant_germline_annotation"
EXOMISER_TASK_TYPE = "exomiser"

# The Ferlab pipeline that produced the slivar VCF -- deliberately not the variant caller
# upstream of this run.
ANNOTATION_PIPELINE = ("Post-processing-Pipeline", "3.0.0")

# 14.0.0 is inherited from existing seed data, not read from the container
# (ferlabcrsj/exomiser:2.4.1, data 2402). Confirm the real software version before this
# becomes load-bearing metadata.
EXOMISER_PIPELINE = ("Exomiser", "14.0.0")

# The spelling already stored in the tenant databases. The casing is odd on purpose:
# matching what is there matters more than being right about it here.
GENOME_BUILD = "GRch38"


def build_patch_body(families: list[Family], collected: dict[str, dict]) -> dict:
    """`families` from resolve_cases, `collected` from collect_outputs (keyed by family id)."""
    cases = []
    for family in families:
        documents = collected[family.family_id]
        slivar_vcf_url = documents["slivar_vcf"]["url"]
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
                        # The same per-member gVCFs the samplesheet fed to the pipeline.
                        "input_documents": [{"url": m.gvcf_url} for m in family.members],
                        "output_documents": [documents[name] for name in ANNOTATION_OUTPUTS],
                    },
                    {
                        "type_code": EXOMISER_TASK_TYPE,
                        "aliquots": [family.proband.aliquot],
                        "pipeline_name": EXOMISER_PIPELINE[0],
                        "pipeline_version": EXOMISER_PIPELINE[1],
                        "genome_build": GENOME_BUILD,
                        # The annotated VCF from the sibling task above, resolved in-batch.
                        #
                        # The run actually feeds Exomiser the *VEP-annotated* VCF
                        # (`exomiser_start_from_vep = true`), one step before slivar. That
                        # file is published as no document, so naming it would fail
                        # TASK-005. Recording the slivar VCF instead puts the lineage one
                        # step downstream of the truth -- a deliberate compromise, see the
                        # DAG doc.
                        "input_documents": [{"url": slivar_vcf_url}],
                        "output_documents": [documents[name] for name in EXOMISER_OUTPUTS],
                    },
                ],
            }
        )
    return {"cases": cases}
