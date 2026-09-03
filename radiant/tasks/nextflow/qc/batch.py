"""Build the `PATCH /{tenant}/cases/batch` body registering a QC run's outputs.

One `quality_control_metrics` task per case, bound to every member's aliquot, with the
alignment's CRAM and index as inputs (documents that already exist in the tenant, so
TASK-005 holds) and the per-family MultiQC set as outputs. PATCH appends: a re-run adds a
second task alongside the first.
"""

from radiant.tasks.nextflow.batch import GENOME_BUILD
from radiant.tasks.nextflow.qc.model import QcCase

QC_TASK_TYPE = "quality_control_metrics"

# The revision the launcher image pins (`QC_PIPELINE_REV` in Dockerfile.nextflow.launcher).
# Bump both together.
QC_PIPELINE = ("quality-control-pipeline", "2.0.0")


def build_patch_body(cases: list[QcCase], collected: dict[str, dict]) -> dict:
    """`cases` from resolve_cases, `collected` from collect (keyed by family id)."""
    body = []
    for case in cases:
        documents = collected[case.family_id]
        inputs = [{"url": url} for m in case.members for url in (m.cram_url, m.crai_url) if url]
        body.append(
            {
                "project_code": case.project_code,
                "submitter_case_id": case.submitter_case_id,
                "tasks": [
                    {
                        "type_code": QC_TASK_TYPE,
                        "aliquots": [m.aliquot for m in case.members],
                        "pipeline_name": QC_PIPELINE[0],
                        "pipeline_version": QC_PIPELINE[1],
                        "genome_build": GENOME_BUILD,
                        "input_documents": inputs,
                        "output_documents": [documents[name] for name in sorted(documents)],
                    }
                ],
            }
        )
    return {"cases": body}
