"""Find what a QC run published for each case, and refuse to register a partial set.

With `cohort_mode = false` the pipeline runs MultiQC once per `familyId` and publishes
`multiqc/{familyId}/` -- confirmed on a real QA run, sizes and all. Two files are registered
under the `aggqc` data type: the report and its parsed-data archive. The per-sample
`qc_json/<aliquot>.metrics.json` sidecars are deliberately *not* registered: everything they
hold is in the archive's tables (General Status, somalier, DRAGEN ploidy), so they only added
documents to each case with no reader. Re-adding them is one entry in `expected_keys`.
"""

from radiant.tasks.nextflow.outputs import MissingOutputsError
from radiant.tasks.nextflow.paths import join_s3
from radiant.tasks.nextflow.qc.model import QcCase

__all__ = ["MissingOutputsError", "expected_keys", "collect"]

MULTIQC_DIR = "multiqc"
DATA_TYPE = "aggqc"
DATA_CATEGORY = "genomic"

REPORT = "multiqc_html"
REPORT_DATA = "multiqc_data"


def expected_keys(case: QcCase) -> dict[str, tuple[str, str]]:
    """`{logical name: (relative key under outdir, format_code)}` for one case."""
    fid = case.family_id
    return {
        REPORT: (f"{MULTIQC_DIR}/{fid}/{fid}_multiqc_report.html", "html"),
        REPORT_DATA: (f"{MULTIQC_DIR}/{fid}/{fid}_multiqc_report_data.zip", "zip"),
    }


def collect(cases: list[QcCase], listing: dict[str, int], outdir_s3: str) -> dict[str, dict]:
    """Match the listing to the cases, keyed by family id. Fails if any case lacks any file."""
    collected, missing = {}, []
    for case in cases:
        documents = {}
        for name, (key, file_format) in expected_keys(case).items():
            if key not in listing:
                missing.append(f"{case.family_id} (case {case.case_id}): {key}")
                continue
            documents[name] = {
                "name": key.rsplit("/", 1)[-1],
                "url": join_s3(outdir_s3, key),
                "size": listing[key],
                "data_category_code": DATA_CATEGORY,
                "data_type_code": DATA_TYPE,
                "format_code": file_format,
            }
        collected[case.family_id] = documents

    if missing:
        raise MissingOutputsError(
            "the pipeline did not publish a complete set of outputs; not registering a "
            "partial case set. Missing under " + outdir_s3 + ":\n  " + "\n  ".join(missing)
        )
    return collected
