import pytest

from radiant.tasks.vcf.experiment import (
    ALIGNMENT_SOMATIC_VARIANT_CALLING_TASK,
    AlignmentSomaticVariantCallingTask,
    build_task_from_dict,
    build_task_from_rows,
)


def _base_row() -> dict:
    return {
        "case_id": 23,
        "task_id": 76,
        "part": 0,
        "task_type": ALIGNMENT_SOMATIC_VARIANT_CALLING_TASK,
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


def test_alignment_somatic_variant_calling_task_single_tumoral_row_ok():
    task = build_task_from_rows([_base_row()])

    assert isinstance(task, AlignmentSomaticVariantCallingTask)
    assert task.task_type == ALIGNMENT_SOMATIC_VARIANT_CALLING_TASK
    assert task.cnv_vcf_filepath == "/path/to/tumor_only.cnv.vcf.gz"
    assert len(task.experiments) == 1
    assert task.experiments[0].histology_type == "tumoral"


def test_alignment_somatic_variant_calling_task_in_two_cases_builds_one_experiment():
    """The task has no case in task_context: a tumor in two somatic cases gives one row per case. The
    occurrences are written once per experiment, so the task keeps a single one."""
    rows = [_base_row(), _base_row() | {"case_id": 24}]

    task = build_task_from_rows(rows)

    assert [exp.seq_id for exp in task.experiments] == [64]
    assert task.cnv_vcf_filepath == "/path/to/tumor_only.cnv.vcf.gz"


def test_alignment_somatic_variant_calling_task_two_experiments_raises():
    """More than one experiment on the task means a tumor-normal analysis mislabelled upstream."""
    row1 = _base_row()
    row2 = _base_row() | {"seq_id": 65, "aliquot": "TCRBOA6_SRX1166091-N", "histology_type": "normal"}

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row1, row2])

    assert ALIGNMENT_SOMATIC_VARIANT_CALLING_TASK in str(excinfo.value)
    assert "[64, 65]" in str(excinfo.value)


def test_alignment_somatic_variant_calling_task_non_tumoral_row_raises():
    """A normal-only task would otherwise write normal depths as tumor values."""
    row = _base_row() | {"histology_type": "normal"}

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row])

    assert "tumoral" in str(excinfo.value)
    assert "normal" in str(excinfo.value)


def test_alignment_somatic_variant_calling_task_missing_histology_type_raises():
    row = _base_row()
    del row["histology_type"]

    with pytest.raises(ValueError):
        build_task_from_rows([row])


def test_alignment_somatic_variant_calling_task_without_cnv_filepath_builds():
    """The staging view's `scnv` gate should always supply the URL, but a missing one must not abort
    the whole partition: every task in the part is built by the same `tasks_output_processor` call."""
    row = _base_row() | {"cnv_vcf_filepath": None}

    task = build_task_from_rows([row])

    assert task.cnv_vcf_filepath is None


def test_alignment_somatic_variant_calling_task_round_trips_through_dict():
    """`import_part` hands tasks to the extraction DAGs as `model_dump()` payloads over XCom."""
    task = build_task_from_rows([_base_row()])

    rebuilt = build_task_from_dict(task.model_dump())

    assert isinstance(rebuilt, AlignmentSomaticVariantCallingTask)
    assert rebuilt == task


def test_tumor_only_variant_calling_is_no_longer_a_task_type():
    """RAD-272: QLIN no longer sends `tumor_only_variant_calling`, and the staging view no longer reads it. A row of
    that type left in staging must be cleaned by the RAD-272 migration: the import refuses it."""
    row = _base_row() | {"task_type": "tumor_only_variant_calling"}

    with pytest.raises(ValueError, match="Unknown task type: tumor_only_variant_calling"):
        build_task_from_rows([row])
