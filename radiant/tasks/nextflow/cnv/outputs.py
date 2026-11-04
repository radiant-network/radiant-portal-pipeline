"""Find what a CNV post-processing run published, and refuse to register a partial one.

Every case runs the pipeline's family route (see `inputs`), so every output is named after
`familyId` and maps back to a case by arithmetic. The Exomiser reports add the proband's
aliquot when Exomiser ran on the proband VCF (`<familyId>.<proband>.exomiser.*`); see
`expected_keys`.

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
# `{prefix}` is the family id, or `<family id>.<proband aliquot>` for the Exomiser reports of
# a family run on its proband VCF (see `expected_keys`).
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
# The index rides along, as for the SNV slivar VCF: the revision the launcher pins
# (`CNV_PIPELINE_REV`) indexes the slivar VCF and publishes the `.tbi` next to it.
#
# The Exomiser reports are `exomiser_cnv`, not `exomiser`: the portal tells them from
# the SNV report, which has the same file names, and the staging view reads only `exomiser`.
OUTPUT_SPEC = {
    "slivar_vcf": (f"{SLIVAR_DIR}/{{prefix}}.cnv.slivar.vcf.gz", "gcnv", "vcf"),
    "slivar_tbi": (f"{SLIVAR_DIR}/{{prefix}}.cnv.slivar.vcf.gz.tbi", "gcnv", "tbi"),
    "exomiser_tsv": (f"{EXOMISER_DIR}/{{prefix}}.exomiser.variants.tsv", "exomiser_cnv", "tsv"),
    "exomiser_html": (f"{EXOMISER_DIR}/{{prefix}}.exomiser.html", "exomiser_cnv", "html"),
    "exomiser_json": (f"{EXOMISER_DIR}/{{prefix}}.exomiser.json", "exomiser_cnv", "json"),
}

ANNOTATION_OUTPUTS = ("slivar_vcf", "slivar_tbi")
EXOMISER_OUTPUTS = ("exomiser_tsv", "exomiser_html", "exomiser_json")


def expected_keys(family_id: str, proband: str) -> dict[str, list[str]]:
    """Relative keys, under the run's outdir, of everything one family must produce: for each
    output, the keys one of which must exist, the preferred one first.

    The slivar VCF is named after the family. An Exomiser report has two possible names
    (cnv-post-processing `docs/output.md`): `<familyId>.<proband>.exomiser.*` when
    Exomiser ran on the proband VCF of the phenopacket's `proband.subject.id` -- `proband` is
    that id, the proband's aliquot as `inputs.build_phenopacket` writes it -- and
    `<familyId>.exomiser.*` when it ran on the family VCF instead, which the pipeline does for
    a phenopacket proband who is not one of the PED's sequenced affected children. Which route
    a family took is the pipeline's call, read off the listing rather than predicted here. A
    family gets one family-mode Exomiser run, so the two names never coexist; the proband one
    is preferred all the same, so the choice is deterministic.
    """
    keys = {}
    for name, (template, _, _) in OUTPUT_SPEC.items():
        prefixes = [f"{family_id}.{proband}", family_id] if name in EXOMISER_OUTPUTS else [family_id]
        keys[name] = [template.format(prefix=prefix) for prefix in prefixes]
    return keys


def collect(families: list[CnvFamily], listing: dict[str, int], outdir_s3: str) -> dict[str, dict]:
    """Match the listing to the families, keyed by family id. Fails if any family lacks any file."""
    collected, missing = {}, []
    for family in families:
        documents = {}
        for name, candidates in expected_keys(family.family_id, family.proband.aliquot).items():
            key = next((candidate for candidate in candidates if candidate in listing), None)
            if key is None:
                missing.append(f"{family.family_id} (case {family.case_id}): {' or '.join(candidates)}")
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
