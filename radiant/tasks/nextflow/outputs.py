"""Find what a pipeline run published, and refuse to register a partial one.

The pipeline names its outputs after `familyId`, which is derived from `cases.id`, so
going from a filename back to a case is arithmetic rather than a lookup.

Sizes come from the S3 listing rather than being trusted from anywhere else: the batch API
compares `size` against any existing document with the same URL and raises DOCUMENT-006 on
a mismatch, so a stale size fails the batch instead of corrupting it.
"""

from radiant.tasks.nextflow.model import Family
from radiant.tasks.nextflow.paths import join_s3

SLIVAR_DIR = "slivar"
EXOMISER_DIR = "exomiser"

# logical name -> (relative key template, data_type_code, format_code)
#
# The VCF must be declared `vcf`, not `gvcf`: `staging_external_sequencing_experiment`
# derives `vcf_filepath` from `format_code='vcf'` on an output doc of a
# `radiant_*_annotation` task, and `exomiser_filepath` from `url LIKE '%variants.tsv'`.
# The html and json match neither predicate -- they are invisible to the ETL and ride
# along purely so the portal can list and serve them.
OUTPUT_SPEC = {
    "slivar_vcf": (f"{SLIVAR_DIR}/variants.{{family_id}}.snv.vep.slivar.vcf.gz", "snv", "vcf"),
    "slivar_tbi": (f"{SLIVAR_DIR}/variants.{{family_id}}.snv.vep.slivar.vcf.gz.tbi", "snv", "tbi"),
    "exomiser_tsv": (f"{EXOMISER_DIR}/{{family_id}}.exomiser.variants.tsv", "exomiser", "tsv"),
    "exomiser_html": (f"{EXOMISER_DIR}/{{family_id}}.exomiser.html", "exomiser", "html"),
    "exomiser_json": (f"{EXOMISER_DIR}/{{family_id}}.exomiser.json", "exomiser", "json"),
}

ANNOTATION_OUTPUTS = ("slivar_vcf", "slivar_tbi")
EXOMISER_OUTPUTS = ("exomiser_tsv", "exomiser_html", "exomiser_json")


class MissingOutputsError(Exception):
    """A pipeline run published less than a complete set for at least one family."""


def expected_keys(family_id: str) -> dict[str, str]:
    """Relative keys, under the run's outdir, of everything one family must produce."""
    return {name: template.format(family_id=family_id) for name, (template, _, _) in OUTPUT_SPEC.items()}


def collect(families: list[Family], listing: dict[str, int], outdir_s3: str) -> dict[str, dict]:
    """Match the listing to the families, keyed by family id.

    Fails if any family is missing any file. A partially-successful Nextflow run must not
    produce a partially-registered case set, and the run's task status is not a reliable
    signal on its own -- outputs can be complete while the task is marked failed, and the
    reverse. Only a per-family assertion over the listing tells the two apart.
    """
    collected, missing = {}, []
    for family in families:
        documents = {}
        for name, key in expected_keys(family.family_id).items():
            if key not in listing:
                missing.append(f"{family.family_id} (case {family.case_id}): {key}")
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
        collected[family.family_id] = documents

    if missing:
        raise MissingOutputsError(
            "the pipeline did not publish a complete set of outputs; not registering a "
            "partial case set. Missing under " + outdir_s3 + ":\n  " + "\n  ".join(missing)
        )
    return collected
