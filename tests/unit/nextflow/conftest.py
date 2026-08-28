"""Row fixtures shaped like what `sql/clinical/pending_annotation_select.sql` returns."""

import pytest

GVCF_BUCKET = "s3://qlin-nextflow-inputs"

_TRIO = [
    ("proband", "affected", 100, "female", "NA12878"),
    ("father", "affected", 101, "male", "NA12891"),
    ("mother", "non_affected", 102, "female", "NA12892"),
]


def member_row(case_id=1072, submitter_case_id="1KGP-1463", role="proband", **overrides):
    row = {
        "case_id": case_id,
        "submitter_case_id": submitter_case_id,
        "primary_condition": "MONDO:0700092",
        "tenant_code": "radiant",
        "project_code": "N1",
        "role": role,
        "affected_status": "affected",
        "patient_id": 100,
        "sex": "female",
        "submitter_patient_id": "PT-100",
        "sample_id": "NA12878",
        "seq_id": 500,
        "aliquot": "NA12878",
        "strategy": "wgs",
        "alignment_task_id": 900,
        "gvcf_url": f"{GVCF_BUCKET}/individuals/NA12878/NA12878.hard-filtered.gvcf.gz",
        "gvcf_matches": 1,
        "exclusion_reason": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def trio_rows():
    """Case 1072: proband + affected father + unaffected mother, deliberately out of order
    so the tests exercise the proband-first sort rather than the query's ORDER BY."""
    rows = [
        member_row(
            role=role,
            affected_status=affected,
            patient_id=patient_id,
            sex=sex,
            sample_id=sample,
            aliquot=sample,
            seq_id=500 + index,
            alignment_task_id=900 + index,
            gvcf_url=f"{GVCF_BUCKET}/individuals/{sample}/{sample}.hard-filtered.gvcf.gz",
        )
        for index, (role, affected, patient_id, sex, sample) in enumerate(_TRIO)
    ]
    return list(reversed(rows))


@pytest.fixture
def singleton_rows():
    """Case 8: one WXS proband, no parents."""
    return [
        member_row(
            case_id=8,
            submitter_case_id="1KGP-HG00096",
            patient_id=200,
            sex="male",
            sample_id="HG00096",
            aliquot="HG00096",
            seq_id=600,
            alignment_task_id=910,
            strategy="wxs",
            gvcf_url=f"{GVCF_BUCKET}/individuals/HG00096/HG00096.hard-filtered.gvcf.gz",
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
