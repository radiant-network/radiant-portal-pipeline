import pytest

from radiant.tasks.vcf.experiment import (
    TUMOR_ONLY_VARIANT_CALLING_TASK,
    TumorOnlyVariantCallingTask,
    build_task_from_dict,
    build_task_from_rows,
)


def _base_row() -> dict:
    return {
        "task_id": 70,
        "part": 0,
        "task_type": TUMOR_ONLY_VARIANT_CALLING_TASK,
        "analysis_type": "somatic",
        "seq_id": 64,
        "patient_id": 62,
        "aliquot": "TCRBOA6_SRX1166091-T",
        "tenant_code": "tenant1",
        "family_role": "proband",
        "affected_status": "affected",
        "sex": "female",
        "experimental_strategy": "wgs",
        "request_priority": "routine",
        "histology_type": "tumoral",
        "cnv_vcf_filepath": "/path/to/tumor_only.cnv.vcf.gz",
        "deleted": False,
    }


def test_tumor_only_variant_calling_task_single_tumoral_row_ok():
    task = build_task_from_rows([_base_row()])

    assert isinstance(task, TumorOnlyVariantCallingTask)
    assert task.task_type == TUMOR_ONLY_VARIANT_CALLING_TASK
    assert task.cnv_vcf_filepath == "/path/to/tumor_only.cnv.vcf.gz"
    assert len(task.experiments) == 1
    assert task.experiments[0].histology_type == "tumoral"


def test_tumor_only_variant_calling_task_multiple_rows_raises():
    """More than one aliquot on the task means a tumor-normal analysis mislabelled upstream."""
    row1 = _base_row()
    row2 = _base_row() | {"seq_id": 65, "aliquot": "TCRBOA6_SRX1166091-N"}

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row1, row2])

    assert "tumor_only_variant_calling" in str(excinfo.value)


def test_tumor_only_variant_calling_task_non_tumoral_row_raises():
    """A normal-only task would otherwise write normal depths as tumor values."""
    row = _base_row() | {"histology_type": "normal"}

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row])

    assert "tumoral" in str(excinfo.value)
    assert "normal" in str(excinfo.value)


def test_tumor_only_variant_calling_task_missing_histology_type_raises():
    row = _base_row()
    del row["histology_type"]

    with pytest.raises(ValueError):
        build_task_from_rows([row])


def test_tumor_only_variant_calling_task_without_cnv_filepath_builds():
    """The staging view's `scnv` gate should always supply the URL, but a missing one must not abort
    the whole partition: every task in the part is built by the same `tasks_output_processor` call."""
    row = _base_row() | {"cnv_vcf_filepath": None}

    task = build_task_from_rows([row])

    assert task.cnv_vcf_filepath is None


def test_tumor_only_variant_calling_task_round_trips_through_dict():
    """`import_part` hands tasks to the extraction DAGs as `model_dump()` payloads over XCom."""
    task = build_task_from_rows([_base_row()])

    rebuilt = build_task_from_dict(task.model_dump())

    assert isinstance(rebuilt, TumorOnlyVariantCallingTask)
    assert rebuilt == task
