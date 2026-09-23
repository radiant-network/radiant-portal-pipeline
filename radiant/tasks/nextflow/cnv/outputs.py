"""Find what a CNV post-processing run published, and refuse to register a partial one.

Every case runs the pipeline's family route (see `inputs`), so every output is named after
`familyId` alone and maps back to a case by arithmetic.

Sizes come from the S3 listing, as for post-processing: the batch API compares `size`
against any existing document with the same URL and raises DOCUMENT-006 on a mismatch.
"""

from radiant.tasks.nextflow.cnv.model import CnvFamily
from radiant.tasks.nextflow.outputs import MissingOutputsError
from radiant.tasks.nextflow.paths import join_s3

__all__ = ["MissingOutputsError", "OUTPUT_SPEC", "ANNOTATION_OUTPUTS", "EXOMISER_OUTPUTS", "expected_keys", "collect"]

SLIVAR_DIR = "slivar"
EXOMISER_DIR = "exomiser"

# logical name -> (relative key template, data_type_code, format_code)
#
# The slivar VCF is the final artefact -- VEP CSQ plus the mode-of-inheritance INFO tags --
# and is declared `gcnv`/`vcf`, the CNV analogue of the annotation DAG's `snv`/`vcf`.
#
# Invisible to the variant ETL, by design for now: `staging_external_sequencing_experiment`
# admits a `gcnv`/`vcf` document only on an `alignment_germline_variant_calling` task, so a
# `radiant_germline_cnv_annotation` output matches none of its predicates and never reaches
# `germline_cnv_occurrence`. Making it visible is one WHERE line there plus a task model,
# and is a separate design.
#
# No index: at the revision the launcher pins (`CNV_PIPELINE_REV`), `SLIVAR_EXPR` publishes
# `*.vcf.gz` only and nothing runs after it. Expecting a `.tbi` here would make every run
# fail in `collect_outputs` after the pipeline had finished. Add a `slivar_tbi` entry (and
# put it in `ANNOTATION_OUTPUTS`) in the same change that bumps the pin to a revision that
# indexes the slivar VCF.
OUTPUT_SPEC = {
    "slivar_vcf": (f"{SLIVAR_DIR}/{{family_id}}.cnv.slivar.vcf.gz", "gcnv", "vcf"),
    "exomiser_tsv": (f"{EXOMISER_DIR}/{{family_id}}.exomiser.variants.tsv", "exomiser", "tsv"),
    "exomiser_html": (f"{EXOMISER_DIR}/{{family_id}}.exomiser.html", "exomiser", "html"),
    "exomiser_json": (f"{EXOMISER_DIR}/{{family_id}}.exomiser.json", "exomiser", "json"),
}

ANNOTATION_OUTPUTS = ("slivar_vcf",)
EXOMISER_OUTPUTS = ("exomiser_tsv", "exomiser_html", "exomiser_json")


def expected_keys(family_id: str) -> dict[str, str]:
    """Relative keys, under the run's outdir, of everything one family must produce."""
    return {name: template.format(family_id=family_id) for name, (template, _, _) in OUTPUT_SPEC.items()}


def collect(families: list[CnvFamily], listing: dict[str, int], outdir_s3: str) -> dict[str, dict]:
    """Match the listing to the families, keyed by family id. Fails if any family lacks any file."""
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
