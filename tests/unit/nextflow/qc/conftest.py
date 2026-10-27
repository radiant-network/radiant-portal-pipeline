"""Row fixtures shaped like what `sql/clinical/pending_quality_control_select.sql` returns:
one row per member."""

import pytest

BUCKET = "s3://qlin-nextflow-inputs"
INPUTS_ROOT = f"{BUCKET}/nextflow/inputs"

_TRIO = [
    ("proband", "affected", 100, "female", "NA12878"),
    ("father", "affected", 101, "male", "NA12891"),
    ("mother", "non_affected", 102, "female", "NA12892"),
]


def alignment_urls(aliquot: str, run: str = "run-42") -> dict[str, str]:
    return {
        "cram": f"{BUCKET}/dragen/{run}/cram/{aliquot}.cram",
        "crai": f"{BUCKET}/dragen/{run}/cram/{aliquot}.cram.crai",
        "tgz": f"{BUCKET}/dragen/{run}/{aliquot}/{aliquot}.extra.tgz",
    }


def member_row(
    case_id=1072,
    submitter_case_id="1KGP-1463",
    role="proband",
    affected_status="affected",
    patient_id=100,
    sex="female",
    aliquot="NA12878",
    seq_id=500,
    alignment_task_id=900,
    strategy="wgs",
    run="run-42",
    exclusion_reason=None,
    documents=True,
    tarball=True,
    **overrides,
):
    """`documents=False` is a member with no alignment; `tarball=False` one whose alignment
    published no DRAGEN metrics tarball."""
    row = {
        "case_id": case_id,
        "submitter_case_id": submitter_case_id,
        "tenant_code": "radiant",
        "project_code": "N1",
        "role": role,
        "affected_status": affected_status,
        "patient_id": patient_id,
        "sex": sex,
        "submitter_patient_id": f"PT-{patient_id}",
        # Deliberately not the aliquot: the samplesheet must only ever carry the aliquot.
        "sample_id": f"S-{aliquot}",
        "seq_id": seq_id,
        "aliquot": aliquot,
        "strategy": strategy,
        "alignment_task_id": alignment_task_id,
        "cram_url": None,
        "cram_matches": 0,
        "crai_url": None,
        "dragen_metrics_url": None,
        "dragen_metrics_matches": 0,
        "exclusion_reason": exclusion_reason,
    }
    if documents:
        urls = alignment_urls(aliquot, run)
        row |= {"cram_url": urls["cram"], "crai_url": urls["crai"], "cram_matches": 1}
        if tarball:
            row |= {"dragen_metrics_url": urls["tgz"], "dragen_metrics_matches": 1}
    return row | overrides


@pytest.fixture
def trio_rows():
    """Case 1072, members deliberately out of order so tests exercise the proband-first sort."""
    rows = [
        member_row(
            role=role,
            affected_status=affected,
            patient_id=patient_id,
            sex=sex,
            aliquot=aliquot,
            seq_id=500 + index,
            alignment_task_id=900 + index,
        )
        for index, (role, affected, patient_id, sex, aliquot) in enumerate(_TRIO)
    ]
    return list(reversed(rows))


@pytest.fixture
def singleton_rows():
    """Case 8: one WXS proband in another DRAGEN run directory, with no metrics tarball."""
    return [
        member_row(
            case_id=8,
            submitter_case_id="1KGP-HG00096",
            patient_id=200,
            sex="male",
            aliquot="HG00096",
            seq_id=600,
            alignment_task_id=910,
            strategy="wxs",
            run="run-43",
            tarball=False,
        )
    ]
