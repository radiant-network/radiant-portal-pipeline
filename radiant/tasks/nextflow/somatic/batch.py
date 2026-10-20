"""Build the `PATCH /{tenant}/cases/batch` body registering a somatic annotation run.

One `radiant_somatic_annotation` task per (calling task, case):

- `aliquots`: the calling task's own, tumor first. The portal derives `task_context` from them, so
  the TO/TN classification of the annotation, and of the import, is the calling task's;
- `input_documents`: the raw `ssnv` VCF, already in the tenant. That link is also what makes the
  calling task stop being pending;
- `output_documents`: the VEP VCF and its index, `ssnv`/`vcf` and `ssnv`/`tbi`;
- `pipeline_name` / `pipeline_version`: the germline annotation's, since it is the same pipeline
  revision. The revision pins the VEP version, cache and flags.

Same backend rules as `radiant.tasks.nextflow.batch`: inputs mandatory (TASK-003), resolvable in
the tenant (TASK-005). PATCH appends: a deliberate re-run adds a second annotation task.
"""

from radiant.tasks.nextflow.batch import ANNOTATION_PIPELINE, GENOME_BUILD
from radiant.tasks.nextflow.somatic.model import CallingTask
from radiant.tasks.nextflow.somatic.outputs import ANNOTATION_OUTPUTS

ANNOTATION_TASK_TYPE = "radiant_somatic_annotation"


def build_patch_body(tasks: list[CallingTask], collected: dict[str, dict], tenant: str) -> dict:
    """`tasks` from select_tasks, `collected` from collect_outputs (keyed by family id). Only the
    cases of `tenant`: the batch endpoint is one tenant per call."""
    cases: dict[int, dict] = {}
    for task in tasks:
        documents = collected[task.family_id]
        annotation = {
            "type_code": ANNOTATION_TASK_TYPE,
            "aliquots": task.aliquots,
            "pipeline_name": ANNOTATION_PIPELINE[0],
            "pipeline_version": ANNOTATION_PIPELINE[1],
            "genome_build": GENOME_BUILD,
            "input_documents": [{"url": task.vcf_url}],
            "output_documents": [documents[name] for name in ANNOTATION_OUTPUTS],
        }
        for case in task.cases:
            if case.tenant_code != tenant:
                continue
            entry = cases.setdefault(
                case.case_id,
                {"project_code": case.project_code, "submitter_case_id": case.submitter_case_id, "tasks": []},
            )
            entry["tasks"].append(annotation)
    return {"cases": [cases[case_id] for case_id in sorted(cases)]}
