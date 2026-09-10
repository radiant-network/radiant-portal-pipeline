"""Row fixtures shaped like what `sql/clinical/pending_quality_control_select.sql` returns:
one row per (member, alignment output document)."""

import pytest

BUCKET = "s3://qlin-nextflow-inputs"
INPUTS_ROOT = f"{BUCKET}/nextflow/inputs"

_TRIO = [
    ("proband", "affected", 100, "female", "NA12878"),
    ("father", "affected", 101, "male", "NA12891"),
    ("mother", "non_affected", 102, "female", "NA12892"),
]


def alignment_urls(aliquot: str, run: str = "run-42") -> dict[str, str]:
    """The layout the probe has to cope with: CRAM in one directory, gVCF in another."""
    return {
        "cram": f"{BUCKET}/dragen/{run}/cram/{aliquot}.cram",
        "crai": f"{BUCKET}/dragen/{run}/cram/{aliquot}.cram.crai",
        "gvcf": f"{BUCKET}/dragen/{run}/{aliquot}/{aliquot}.hard-filtered.gvcf.gz",
        "tbi": f"{BUCKET}/dragen/{run}/{aliquot}/{aliquot}.hard-filtered.gvcf.gz.tbi",
    }


def document_rows(
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
    **overrides,
):
    base = {
        "case_id": case_id,
        "submitter_case_id": submitter_case_id,
        "tenant_code": "radiant",
        "project_code": "N1",
        "role": role,
        "affected_status": affected_status,
        "patient_id": patient_id,
        "sex": sex,
        "submitter_patient_id": f"PT-{patient_id}",
        "sample_id": aliquot,
        "seq_id": seq_id,
        "aliquot": aliquot,
        "strategy": strategy,
        "alignment_task_id": alignment_task_id,
        "cram_url": None,
        "cram_matches": 0,
        "crai_url": None,
        "document_url": None,
        "document_data_type": None,
        "document_format": None,
        "exclusion_reason": exclusion_reason,
    }
    base.update(overrides)
    if not documents:
        return [base]
    urls = alignment_urls(aliquot, run)
    base["cram_url"], base["crai_url"], base["cram_matches"] = urls["cram"], urls["crai"], 1
    types = {
        "cram": ("alignment", "cram"),
        "crai": ("alignment", "crai"),
        "gvcf": ("snv", "gvcf"),
        "tbi": ("snv", "tbi"),
    }
    return [
        base | {"document_url": urls[k], "document_data_type": types[k][0], "document_format": types[k][1]}
        for k in ("cram", "crai", "gvcf", "tbi")
    ]


@pytest.fixture
def trio_rows():
    """Case 1072, members deliberately out of order so tests exercise the proband-first sort."""
    rows = []
    for index, (role, affected, patient_id, sex, aliquot) in enumerate(_TRIO):
        rows += document_rows(
            role=role,
            affected_status=affected,
            patient_id=patient_id,
            sex=sex,
            aliquot=aliquot,
            seq_id=500 + index,
            alignment_task_id=900 + index,
        )
    return list(reversed(rows))


@pytest.fixture
def singleton_rows():
    """Case 8: one WXS proband in another DRAGEN run directory."""
    return document_rows(
        case_id=8,
        submitter_case_id="1KGP-HG00096",
        patient_id=200,
        sex="male",
        aliquot="HG00096",
        seq_id=600,
        alignment_task_id=910,
        strategy="wxs",
        run="run-43",
    )


def fake_listers(files: dict[str, set[str]]):
    """`{s3 dir: {filenames}}` -> the two lister callables `locate_metrics` takes."""

    def list_dir(uri: str) -> set[str]:
        return set(files.get(uri.rstrip("/"), set()))

    def list_tree(uri: str) -> list[str]:
        prefix = uri.rstrip("/") + "/"
        return [name for directory, names in files.items() if (directory + "/").startswith(prefix) for name in names]

    return list_dir, list_tree
