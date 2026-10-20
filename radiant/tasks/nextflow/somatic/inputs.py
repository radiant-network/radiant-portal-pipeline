"""Build the pipeline's only input artefact: the samplesheet.

The run enters snv-post-processing at its `normalize` step, whose samplesheet takes `familyId`,
`sequencingType` and `vcf` (the second `oneOf` branch of the pipeline's
`assets/schema_input.json`). No PED, no phenopacket: slivar and Exomiser are not run.

`vcf` is a **pod path**, not an S3 URI: the pipeline stats it at launch (`exists: true` in its
schema), so a VCF registered outside the workspace bucket fails here, in `to_mount`, naming its
calling task, rather than in the driver pod.
"""

import csv
import io

from radiant.tasks.nextflow.paths import to_mount
from radiant.tasks.nextflow.somatic.model import CallingTask

SAMPLESHEET_COLUMNS = ["familyId", "sequencingType", "vcf"]


def build_inputs(tasks: list[CallingTask], inputs_root: str, inputs_mount: str) -> dict[str, str]:
    """Return `{relative key: file content}` for the whole run, as the other cases DAGs do."""
    return {"samplesheet.csv": build_samplesheet(tasks, inputs_root, inputs_mount)}


def build_samplesheet(tasks: list[CallingTask], inputs_root: str, inputs_mount: str) -> str:
    """One row per calling task. The whole run fails on the first VCF that is not on the mount:
    every such task is named, so the run is fixed once rather than once per task."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SAMPLESHEET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    errors = []
    for task in tasks:
        try:
            vcf = to_mount(task.vcf_url, inputs_root, inputs_mount)
        except ValueError as error:
            errors.append(f"calling task {task.task_id}: {error}")
            continue
        writer.writerow({"familyId": task.family_id, "sequencingType": task.sequencing_type, "vcf": vcf})
    if errors:
        raise ValueError("cannot build the samplesheet:\n  " + "\n  ".join(errors))
    return buffer.getvalue()
