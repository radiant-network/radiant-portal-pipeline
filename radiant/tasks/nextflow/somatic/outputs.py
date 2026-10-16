"""Find what a somatic annotation run published, and refuse to register a partial one.

The pipeline names its VEP output after `familyId`, which is the calling task id, so a file maps
back to its task by arithmetic.

Sizes come from the S3 listing, as for the other cases DAGs: the batch API compares `size` against
any existing document with the same URL and raises DOCUMENT-006 on a mismatch.
"""

from radiant.tasks.nextflow.outputs import MissingOutputsError
from radiant.tasks.nextflow.paths import join_s3
from radiant.tasks.nextflow.somatic.model import CallingTask

__all__ = ["MissingOutputsError", "OUTPUT_SPEC", "ANNOTATION_OUTPUTS", "expected_keys", "collect"]

VEP_DIR = "ensemblvep"

# logical name -> (relative key template, data_type_code, format_code)
#
# The VEP VCF is the final artefact: the run stops after VEP (`tools = vep`). It is declared
# `ssnv`/`vcf`, which is what `staging_external_sequencing_experiment` reads as `vcf_filepath` on a
# `radiant_somatic_annotation` task. The index rides along because the loader reads the `.tbi`
# next to the VCF; the pipeline indexes the VEP output and publishes both.
OUTPUT_SPEC = {
    "vep_vcf": (f"{VEP_DIR}/variants.{{family_id}}.snv.vep.vcf.gz", "ssnv", "vcf"),
    "vep_tbi": (f"{VEP_DIR}/variants.{{family_id}}.snv.vep.vcf.gz.tbi", "ssnv", "tbi"),
}

ANNOTATION_OUTPUTS = ("vep_vcf", "vep_tbi")


def expected_keys(family_id: str) -> dict[str, str]:
    """Relative keys, under the run's outdir, of everything one calling task must produce."""
    return {name: template.format(family_id=family_id) for name, (template, _, _) in OUTPUT_SPEC.items()}


def collect(tasks: list[CallingTask], listing: dict[str, int], outdir_s3: str) -> dict[str, dict]:
    """Match the listing to the calling tasks, keyed by family id. Fails if any task lacks any
    file: a partially-successful run must not produce a partially-registered set."""
    collected, missing = {}, []
    for task in tasks:
        documents = {}
        for name, key in expected_keys(task.family_id).items():
            if key not in listing:
                missing.append(f"calling task {task.task_id} ({task.vcf_url}): {key}")
                continue
            _, data_type, file_format = OUTPUT_SPEC[name]
            documents[name] = {
                "name": key.rsplit("/", 1)[-1],
                "url": join_s3(outdir_s3, key),
                "size": listing[key],
                "data_category_code": "genomic",
                "data_type_code": data_type,
                "format_code": file_format,
            }
        collected[task.family_id] = documents

    if missing:
        raise MissingOutputsError(
            "the pipeline did not publish a complete set of outputs; not registering a "
            "partial set. Missing under " + outdir_s3 + ":\n  " + "\n  ".join(missing)
        )
    return collected
