import pytest

from radiant.tasks.vcf.experiment import (
    ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK,
    AlignmentGermlineVariantCallingTask,
    build_task_from_rows,
)


def _base_row() -> dict:
    return {
        "task_id": 1000181,
        "part": 0,
        "task_type": ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK,
        "analysis_type": "germline",
        "seq_id": 1000049,
        "patient_id": 1000020,
        "aliquot": "A1000049",
        "tenant_code": "tenant1",
        "family_role": "proband",
        "affected_status": "affected",
        "sex": "female",
        "experimental_strategy": "wgs",
        "request_priority": "routine",
        "histology_type": "normal",
        "cnv_vcf_filepath": "s3://bucket/A1000049.cnv.vcf.gz",
        "deleted": False,
    }


def test_alignment_task_single_row():
    task = build_task_from_rows([_base_row()])

    assert isinstance(task, AlignmentGermlineVariantCallingTask)
    assert task.cnv_vcf_filepath == "s3://bucket/A1000049.cnv.vcf.gz"
    assert [e.seq_id for e in task.experiments] == [1000049]


def test_alignment_task_shared_by_two_germline_cases_gives_one_experiment():
    """RAD-266: the experiment is in two germline cases, proband in one, sibling in the other. The task has no case,
    so it comes with one row per case: the occurrences must still be written once."""
    rows = [_base_row(), _base_row() | {"family_role": "sibling", "affected_status": "non_affected"}]

    task = build_task_from_rows(rows)

    assert [(e.seq_id, e.family_role) for e in task.experiments] == [(1000049, "proband")]
    assert task.cnv_vcf_filepath == "s3://bucket/A1000049.cnv.vcf.gz"


@pytest.mark.parametrize(
    "other",
    [
        {"seq_id": 1000050, "aliquot": "A1000050"},
        {"cnv_vcf_filepath": "s3://bucket/other.cnv.vcf.gz"},
    ],
)
def test_alignment_task_with_two_experiments_or_two_files_raises(other):
    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([_base_row(), _base_row() | other])

    assert "alignment_germline_variant_calling" in str(excinfo.value)
    assert "1000181" in str(excinfo.value)
