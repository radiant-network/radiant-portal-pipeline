"""Find what a QC run published for each case, and refuse to register a partial set.

With `cohort_mode = false` the pipeline runs MultiQC once per `familyId` and publishes
`multiqc/{familyId}/`. Registered as `aggqc`: the report, its parsed-data archive and one
`qc_json/<aliquot>.metrics.json` per member, named from the case's own aliquots so that a sample
outside the case is never registered. The portal hides them from its files list and lab manifest.

A somatic case may be left out on its own and stays pending; the other cases are registered. A
tumor with no DRAGEN file named after its aliquot has no report (dropped on the last poll); one
without `<aliquot>.mapping_metrics.csv` has no `dragen_metrics.mapping_metrics` in its metrics.json
(dropped too). A germline set is not checked: a missing one still fails the run.
"""

import json
import logging

from radiant.tasks.nextflow.outputs import MissingOutputsError
from radiant.tasks.nextflow.paths import join_s3
from radiant.tasks.nextflow.qc.model import QcCase

__all__ = [
    "MissingOutputsError",
    "expected_keys",
    "collect",
    "tumor_metrics_keys",
    "check_tumor_metrics",
    "has_mapping_metrics",
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
        keys[f"{METRICS_JSON}_{member.aliquot}"] = (_metrics_json_key(fid, member.aliquot), "json")
    return keys


def _metrics_json_key(family_id: str, aliquot: str) -> str:
    return f"{MULTIQC_DIR}/{family_id}/qc_json/{aliquot}.metrics.json"


def collect(
    cases: list[QcCase], listing: dict[str, int], outdir_s3: str, *, last_attempt: bool = False
) -> dict[str, dict]:
    """Match the listing to the cases, keyed by family id. Fails if any case lacks any file, except
    on the `last_attempt`, where a somatic case lacking files is left out and logged at ERROR."""
    collected, missing = {}, []
    for case in cases:
        documents = {}
        expected = expected_keys(case)
        absent = [key for key, _ in expected.values() if key not in listing]
        if absent and last_attempt and case.is_somatic:
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


def tumor_metrics_keys(cases: list[QcCase]) -> dict[str, str]:
    """`{family id: relative key under outdir}` of each somatic case's tumor metrics.json."""
    return {c.family_id: _metrics_json_key(c.family_id, c.proband.aliquot) for c in cases if c.is_somatic}


def check_tumor_metrics(cases: list[QcCase], collected: dict[str, dict], metrics: dict[str, bytes]) -> dict[str, dict]:
    """`collected` without the somatic cases whose tumor metrics.json, from `metrics` (keyed like
    `tumor_metrics_keys`), holds no DRAGEN mapping metrics. Each case dropped is logged at ERROR."""
    kept = dict(collected)
    for case in cases:
        if not case.is_somatic or has_mapping_metrics(metrics.get(case.family_id)):
            continue
        LOGGER.error(
            "case %d, aliquot %s: no DRAGEN mapping metrics in %s, so its DRAGEN metrics tarball holds no "
            "mapping_metrics.csv named after the aliquot. Not registered: the case stays pending until its "
            "data is fixed",
            case.case_id,
            case.proband.aliquot,
            _metrics_json_key(case.family_id, case.proband.aliquot),
        )
        kept.pop(case.family_id, None)
    return kept


def has_mapping_metrics(content: bytes | str | None) -> bool:
    """`dragen_metrics.mapping_metrics` is a non-empty object. Unreadable content has none.

    Not `qc_metrics.aln_metrics`: its coverage fields come from `wgs_coverage_metrics.csv`, so they
    are filled for a tarball that lacks only the mapping CSV.
    """
    try:
        mapping_metrics = json.loads(content)["dragen_metrics"]["mapping_metrics"]
    except (TypeError, ValueError, KeyError):
        return False
    return isinstance(mapping_metrics, dict) and bool(mapping_metrics)


def kept_cases(cases: list[QcCase], collected: dict[str, dict]) -> list[QcCase]:
    """The cases to register: the ones `collected` still holds."""
    return [c for c in cases if c.family_id in collected]
