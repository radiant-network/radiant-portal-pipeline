"""Row fixtures shaped like what `sql/clinical/pending_cnv_annotation_select.sql` returns:
one row per (case, member), carrying the alignment's CNV VCF and, when present, its CRAM."""

import pytest

BUCKET = "s3://qlin-nextflow-inputs"

_TRIO = [
    ("proband", "affected", 100, "female", "NA12878"),
    ("father", "affected", 101, "male", "NA12891"),
    ("mother", "non_affected", 102, "female", "NA12892"),
]


def alignment_urls(aliquot: str, run: str = "run-42") -> dict[str, str]:
    return {
        "gcnv": f"{BUCKET}/dragen/{run}/{aliquot}/{aliquot}.cnv.vcf.gz",
        "cram": f"{BUCKET}/dragen/{run}/cram/{aliquot}.cram",
        "crai": f"{BUCKET}/dragen/{run}/cram/{aliquot}.cram.crai",
    }


def member_row(case_id=1072, submitter_case_id="1KGP-1463", role="proband", aliquot="NA12878", **overrides):
    urls = alignment_urls(aliquot)
    row = {
        "case_id": case_id,
        "submitter_case_id": submitter_case_id,
        "tenant_code": "radiant",
        "project_code": "N1",
        "role": role,
        "affected_status": "affected",
        "patient_id": 100,
        "sex": "female",
        "submitter_patient_id": "PT-100",
        "sample_id": aliquot,
        "seq_id": 500,
        "aliquot": aliquot,
        "strategy": "wgs",
        "alignment_task_id": 900,
        "alignment_pipeline": "Dragen",
        "gcnv_url": urls["gcnv"],
        "gcnv_matches": 1,
        "cram_url": urls["cram"],
        "crai_url": urls["crai"],
        "exclusion_reason": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def trio_rows():
    """Case 1072: proband + affected father + unaffected mother, every member with a CNV VCF
    and a CRAM/index pair, deliberately out of order so the proband-first sort is exercised."""
    rows = [
        member_row(
            role=role,
            affected_status=affected,
            patient_id=patient_id,
            sex=sex,
            aliquot=sample,
            seq_id=500 + index,
            alignment_task_id=900 + index,
        )
        for index, (role, affected, patient_id, sex, sample) in enumerate(_TRIO)
    ]
    return list(reversed(rows))


@pytest.fixture
def singleton_rows():
    """Case 8: one WXS proband with a CNV VCF but no CRAM registered."""
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
            cram_url=None,
            crai_url=None,
        )
    ]


@pytest.fixture
def phenotype_rows():
    return [
        {
            "case_id": 1072,
            "patient_id": 100,
            "hpo_id": "HP:0001249",
            "hpo_label": "Intellectual disability",
            "onset_code": None,
            "interpretation_code": "positive",
        },
        {
            "case_id": 1072,
            "patient_id": 100,
            "hpo_id": "HP:0000618",
            "hpo_label": "Blindness",
            "onset_code": None,
            "interpretation_code": "negative",
        },
        # The father's terms: the phenopacket carries the proband's only.
        {
            "case_id": 1072,
            "patient_id": 101,
            "hpo_id": "HP:0001250",
            "hpo_label": "Seizure",
            "onset_code": None,
            "interpretation_code": "positive",
        },
    ]
