import pytest

from radiant.tasks.vcf.experiment import (
    AlignmentGermlineVariantCallingTask,
    Experiment,
    RadiantGermlineAnnotationTask,
    build_task_from_dict,
    build_task_from_rows,
)


def _base_row(task_type: str) -> dict:
    return {
        "task_id": 1,
        "part": 0,
        "task_type": task_type,
        "analysis_type": "germline",
        "seq_id": 123,
        "patient_id": 456,
        "aliquot": "A1",
        "tenant_code": "tenant1",
        "family_role": "proband",
        "affected_status": "affected",
        "sex": "male",
        "experimental_strategy": "wgs",
        "request_priority": "routine",
        "deleted": False,
    }


def test_build_experiment_from_row():
    row = _base_row("radiant_germline_annotation")
    exp = Experiment(
        seq_id=row["seq_id"],
        patient_id=row["patient_id"],
        aliquot=row["aliquot"],
        tenant_code=row["tenant_code"],
        family_role=row["family_role"],
        affected_status=row["affected_status"],
        sex=row["sex"],
        experimental_strategy=row["experimental_strategy"],
        request_priority=row["request_priority"],
    )
    assert exp.seq_id == 123
    assert exp.patient_id == 456
    assert exp.aliquot == "A1"
    assert exp.family_role == "proband"
    assert exp.request_priority == "routine"


def test_radiant_germline_annotation_task_build_from_rows_proband_present():
    row = _base_row("radiant_germline_annotation")
    row["vcf_filepath"] = "/path/to/proband.vcf"
    row["index_vcf_filepath"] = "/path/to/proband.vcf.idx"

    task = build_task_from_rows([row])

    assert isinstance(task, RadiantGermlineAnnotationTask)
    assert task.task_id == 1
    assert task.part == 0
    assert task.task_type == "radiant_germline_annotation"
    assert task.analysis_type == "germline"
    assert len(task.experiments) == 1
    assert task.experiments[0].seq_id == row["seq_id"]
    assert task.vcf_filepath == "/path/to/proband.vcf"
    assert task.index_vcf_filepath == "/path/to/proband.vcf.idx"


def test_radiant_germline_annotation_task_reads_the_exomiser_file_from_the_proband_row():
    """RAD-116: the staging view sets the Exomiser file on the proband's row only."""
    proband = _base_row("radiant_germline_annotation")
    proband["vcf_filepath"] = "/path/to/family.vcf"
    proband["exomiser_filepath"] = "/path/to/family.exomiser.variants.tsv"
    mother = _base_row("radiant_germline_annotation")
    mother.update(seq_id=124, family_role="mother", vcf_filepath="/path/to/family.vcf", exomiser_filepath=None)

    task = build_task_from_rows([mother, proband])

    assert isinstance(task, RadiantGermlineAnnotationTask)
    assert task.exomiser_filepath == "/path/to/family.exomiser.variants.tsv"
    assert len(task.experiments) == 2


def test_radiant_germline_annotation_task_without_exomiser_file():
    row = _base_row("radiant_germline_annotation")
    row["vcf_filepath"] = "/path/to/proband.vcf"

    task = build_task_from_rows([row])

    assert task.exomiser_filepath is None


def test_radiant_germline_annotation_task_build_from_rows_no_proband_raises():
    row = _base_row("radiant_germline_annotation")
    row["family_role"] = "mother"
    row["vcf_filepath"] = "/path/to/mother.vcf"

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row])

    assert "No proband found" in str(excinfo.value)


def test_alignment_germline_variant_calling_task_single_row_ok():
    row = _base_row("alignment_germline_variant_calling")
    row["cnv_vcf_filepath"] = "/path/to/cnv.vcf"

    task = build_task_from_rows([row])

    assert isinstance(task, AlignmentGermlineVariantCallingTask)
    assert task.cnv_vcf_filepath == "/path/to/cnv.vcf"
    assert len(task.experiments) == 1


def test_alignment_germline_variant_calling_task_multiple_rows_raises():
    row1 = _base_row("alignment_germline_variant_calling")
    row1["cnv_vcf_filepath"] = "/path/to/cnv1.vcf"

    row2 = _base_row("alignment_germline_variant_calling")
    row2["cnv_vcf_filepath"] = "/path/to/cnv2.vcf"
    row2["seq_id"] = 999

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row1, row2])

    assert "alignment_germline_variant_calling" in str(excinfo.value)


def test_legacy_exomiser_task_type_is_unknown():
    """RAD-116: the `exomiser` task type is gone; its file is an output of the annotation task."""
    row = _base_row("exomiser")
    row["exomiser_filepath"] = "/path/to/exomiser.variants.tsv"

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row])

    assert "Unknown task type" in str(excinfo.value)


def test_build_task_from_rows_empty_raises():
    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([])

    assert "No rows provided" in str(excinfo.value)


def test_build_task_from_rows_inconsistent_task_types_raises():
    row1 = _base_row("radiant_germline_annotation")
    row1["vcf_filepath"] = "/path/to/proband.vcf"

    row2 = _base_row("alignment_germline_variant_calling")
    row2["cnv_vcf_filepath"] = "/path/to/cnv.vcf"

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row1, row2])

    assert "Inconsistent task types in rows, found:" in str(excinfo.value)
    assert "alignment_germline_variant_calling" in str(excinfo.value)
    assert "radiant_germline_annotation" in str(excinfo.value)


def test_build_task_from_rows_unknown_task_type_raises():
    row = _base_row("unknown_task_type")

    with pytest.raises(ValueError) as excinfo:
        build_task_from_rows([row])

    assert "Unknown task type" in str(excinfo.value)


def test_build_task_from_dict_radiant_germline_annotation():
    task_dict = {
        "task_type": "radiant_germline_annotation",
        "task_id": 1,
        "part": 0,
        "analysis_type": "germline",
        "experiments": [
            {
                "seq_id": 123,
                "patient_id": 456,
                "aliquot": "A1",
                "tenant_code": "tenant1",
                "family_role": "proband",
                "affected_status": "affected",
                "sex": "male",
                "experimental_strategy": "wgs",
                "request_priority": "routine",
            }
        ],
        "vcf_filepath": "/path/to/proband.vcf",
        "index_vcf_filepath": "/path/to/proband.vcf.idx",
        "deleted": False,
    }

    task = build_task_from_dict(task_dict)

    assert isinstance(task, RadiantGermlineAnnotationTask)
    assert task.vcf_filepath == "/path/to/proband.vcf"
    assert task.index_vcf_filepath == "/path/to/proband.vcf.idx"
    assert task.experiments[0].seq_id == 123


def test_build_task_from_dict_unknown_task_type_raises():
    task_dict = {
        "task_type": "unknown_task_type",
        "task_id": 1,
        "part": 0,
        "analysis_type": "germline",
        "experiments": [],
    }

    with pytest.raises(ValueError) as excinfo:
        build_task_from_dict(task_dict)

    assert "Unknown task type" in str(excinfo.value)
