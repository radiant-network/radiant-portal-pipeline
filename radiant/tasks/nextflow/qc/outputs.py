"""Find what a QC run published for each case, and refuse to register a partial set.

With `cohort_mode = false` the pipeline runs MultiQC once per `familyId` and publishes
`multiqc/{familyId}/`. Registered as `aggqc`: the report, its parsed-data archive and one
`qc_json/<aliquot>.metrics.json` per member, named from the case's own aliquots so that a sample
outside the case is never registered. The portal hides them from its files list and lab manifest.

A tumor with no DRAGEN file named after its aliquot gets no report: that somatic case is left out
and stays pending, and the other cases are registered. A missing germline set still fails the run.
"""

import logging

from radiant.tasks.nextflow.outputs import MissingOutputsError
from radiant.tasks.nextflow.paths import join_s3
from radiant.tasks.nextflow.qc.model import QcCase

__all__ = [
    "MissingOutputsError",
    "expected_keys",
    "collect",
    "kept_cases",
]

LOGGER = logging.getLogger(__name__)

MULTIQC_DIR = "multiqc"
DATA_TYPE = "aggqc"
DATA_CATEGORY = "genomic"

REPORT = "multiqc_html"
REPORT_DATA = "multiqc_data"
METRICS_JSON = "metrics_json"


def expected_keys(case: QcCase) -> dict[str, tuple[str, str]]:
    """`{logical name: (relative key under outdir, format_code)}` for one case."""
    fid = case.family_id
    keys = {
        REPORT: (f"{MULTIQC_DIR}/{fid}/{fid}_multiqc_report.html", "html"),
        REPORT_DATA: (f"{MULTIQC_DIR}/{fid}/{fid}_multiqc_report_data.zip", "zip"),
    }
    for member in case.members:
        keys[f"{METRICS_JSON}_{member.aliquot}"] = (
            f"{MULTIQC_DIR}/{fid}/qc_json/{member.aliquot}.metrics.json",
            "json",
        )
    return keys


def collect(cases: list[QcCase], listing: dict[str, int], outdir_s3: str) -> dict[str, dict]:
    """Match the listing to the cases, keyed by family id. Fails if a germline case lacks any file;
    a somatic case lacking files is left out and logged at ERROR."""
    collected, missing = {}, []
    for case in cases:
        documents = {}
        expected = expected_keys(case)
        absent = [key for key, _ in expected.values() if key not in listing]
        if absent and case.is_somatic:
            LOGGER.error(
                "case %d, aliquot %s: no report under %s (missing %s), so its DRAGEN metrics tarball holds "
                "no file named after the aliquot. Not registered: the case stays pending until its data is fixed",
                case.case_id,
                case.proband.aliquot,
                outdir_s3,
                ", ".join(absent),
            )
            continue
        missing += [f"{case.family_id} (case {case.case_id}): {key}" for key in absent]
        for name, (key, file_format) in expected.items():
            if key not in listing:
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


def kept_cases(cases: list[QcCase], collected: dict[str, dict]) -> list[QcCase]:
    """The cases to register: the ones `collected` still holds."""
    return [c for c in cases if c.family_id in collected]
