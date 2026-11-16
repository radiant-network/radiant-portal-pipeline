"""Find what a CNV post-processing run published, and refuse to register a partial one.

Every case runs the pipeline's family route (see `inputs`), so every output is named after
`familyId` and maps back to a case by arithmetic. The outputs add the proband's aliquot when the
pipeline built them from the proband VCF (`<familyId>.<proband>.*`); see `expected_keys`.

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
# `{prefix}` is `<family id>.<proband aliquot>` for a family run on its proband VCF, or the
# family id alone (see `expected_keys`).
#
# The slivar VCF is the final artefact -- VEP CSQ plus the mode-of-inheritance INFO tags --
# and is declared `gcnv`/`vcf`, the CNV analogue of the annotation DAG's `snv`/`vcf`.
#
# `staging_external_sequencing_experiment` admits it, on the
# `radiant_germline_cnv_annotation` task, and `import_part` loads the proband's records into
# `germline_cnv_occurrence` (`radiant/tasks/vcf/cnv/germline/process.py`).
#
# The index rides along, as for the SNV slivar VCF: the revision the launcher pins
# (`CNV_PIPELINE_REV`) indexes the slivar VCF and publishes the `.tbi` next to it.
#
# The Exomiser reports are `exomiser_cnv`, not `exomiser`: the portal tells them from
# the SNV report, which has the same file names. The staging view admits `exomiser_cnv` on the
# `radiant_germline_cnv_annotation` task only (RAD-181): `import_part` loads it into
# `raw_exomiser_cnv`, never into the SNV `raw_exomiser`.
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

    Every output has two possible names (cnv-post-processing `docs/output.md`):
    `<familyId>.<proband>.*` when the pipeline built it from the proband VCF -- `proband` is the
    phenopacket's `proband.subject.id`, the proband's aliquot as `inputs.build_phenopacket` writes
    it -- and `<familyId>.*` when it used the family VCF, which the pipeline does for a family
    whose proband is not an affected child with a parent in the PED (a singleton, or an unaffected
    proband). Which route a family took is the pipeline's call, read off the listing rather than
    predicted here. Another affected
    child gets its own `<familyId>.<child>.*` slivar VCF, which is not this case's. The proband
    name is preferred, so the choice is deterministic.
    """
    prefixes = [f"{family_id}.{proband}", family_id]
    return {
        name: [template.format(prefix=prefix) for prefix in prefixes] for name, (template, _, _) in OUTPUT_SPEC.items()
    }


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
