"""Build the QC pipeline's one input artefact: the samplesheet.

No PED and no phenopacket: the pipeline derives its pedigree from the `relationship_to_proband`,
`sex` and `affected_status` columns, and QC has no use for HPO terms.

File paths are **pod paths**, not S3 URIs, and the pipeline stats every one of them at launch
(`exists: true` in its schema, `checkIfExists` for `dragenMetrics`), so a CRAM or a tarball
registered outside the workspace bucket fails here, in `to_mount`, rather than in the driver pod.

`dragenMetrics` is never empty: an empty value makes the pipeline recompute BAM_QC / VCF_QC for
that sample, hours of worker pods nobody asked for. It is the alignment's DRAGEN metrics tarball
or, when the alignment published none, the CRAM's directory. That fallback is not checked: the
pipeline keeps only the files named after the sample, so a directory without them gives a green
run with empty alignment sections. Each fallback is logged so it can be traced back.
"""

import csv
import io
import logging

from radiant.tasks.nextflow.paths import to_mount
from radiant.tasks.nextflow.qc.model import QcCase, QcMember
from radiant.tasks.nextflow.qc.resolve import strategy_of

LOGGER = logging.getLogger(__name__)

# Fixed by the pipeline's `assets/schema_input.json`. `lane` and `runId` are omitted: one
# CRAM per sample, nothing to merge.
SAMPLESHEET_COLUMNS = [
    "participant",
    "sample",
    "familyId",
    "fileType",
    "file1",
    "file2",
    "experimentalStrategy",
    "sex",
    "status",
    "relationship_to_proband",
    "affected_status",
    "dragenMetrics",
]

SEX = {"male": "Male", "female": "Female", "unknown": "NA"}
AFFECTED = {"affected": "Affected", "non_affected": "Unaffected", "unknown": "Unknown"}
# The pipeline's enum is wider (half-siblings, twins, children); the clinical value set is
# not. Anything the clinical model can say that the pipeline cannot name becomes `Other`.
RELATIONSHIP = {
    "proband": "Proband",
    "mother": "Mother",
    "father": "Father",
    "brother": "Brother",
    "sister": "Sister",
    "fetus": "Fetus",
}
# Germline: every sample is a normal.
STATUS_NORMAL = "0"


def participant_id(member: QcMember) -> str:
    """`P<patient id>`. Not the submitter id (free text, may carry spaces the schema rejects)
    and not the bare id (nf-schema's CSV reader coerces a numeric string to a number)."""
    return f"P{member.patient_id}"


def dragen_metrics_url(member: QcMember) -> str:
    """The S3 url the `dragenMetrics` column points at: the tarball, else the CRAM's directory."""
    if member.dragen_metrics_url:
        return member.dragen_metrics_url
    return member.cram_url.rsplit("/", 1)[0]


def build_inputs(cases: list[QcCase], inputs_root: str, inputs_mount: str) -> dict[str, str]:
    """`{relative key: content}` for one launcher run -- a single samplesheet."""
    return {"samplesheet.csv": build_samplesheet(cases, inputs_root, inputs_mount)}


def build_samplesheet(cases: list[QcCase], inputs_root: str, inputs_mount: str) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SAMPLESHEET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for case in cases:
        for member in case.members:
            if not member.dragen_metrics_url:
                LOGGER.warning(
                    "case %d, aliquot %s: no DRAGEN metrics tarball, the pipeline reads %s",
                    case.case_id,
                    member.aliquot,
                    dragen_metrics_url(member),
                )
            writer.writerow(
                {
                    "participant": participant_id(member),
                    # The aliquot, because DRAGEN names its metric files after it and the
                    # pipeline matches on the exact first dot-token.
                    "sample": member.aliquot,
                    "familyId": case.family_id,
                    "fileType": "CRAM",
                    "file1": to_mount(member.cram_url, inputs_root, inputs_mount),
                    "file2": to_mount(member.crai_url, inputs_root, inputs_mount) if member.crai_url else "",
                    "experimentalStrategy": strategy_of(member),
                    "sex": SEX.get(member.sex, "NA"),
                    "status": STATUS_NORMAL,
                    "relationship_to_proband": RELATIONSHIP.get(member.role, "Other"),
                    "affected_status": AFFECTED.get(member.affected_status, "Unknown"),
                    "dragenMetrics": to_mount(dragen_metrics_url(member), inputs_root, inputs_mount),
                }
            )
    return buffer.getvalue()
