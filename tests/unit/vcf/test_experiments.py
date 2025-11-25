import pytest
from pydantic import ValidationError

from radiant.tasks.vcf.experiment import (
    AlignmentGermlineVariantCallingTask,
    ExomiserTask,
    Experiment,
    RadiantGermlineAnnotationTask,
)


def make_experiment(**overrides) -> Experiment:
    data = {
        "seq_id": 1,
        "patient_id": 42,
        "aliquot": "A1",
        "family_role": "proband",
        "affected_status": "affected",
        "sex": "male",
        "experimental_strategy": "wgs",
        "request_priority": "high",
    }
    data.update(overrides)
    return Experiment(**data)


def test_experiment_creation_and_defaults():
    exp = make_experiment(request_priority=None)
    assert exp.seq_id == 1
    assert exp.patient_id == 42
    assert exp.request_priority is None


def test_experiment_is_frozen():
    exp = make_experiment()
    with pytest.raises(ValidationError):
        exp.seq_id = 2


def test_base_task_with_experiments():
    exp1 = make_experiment(seq_id=1)
    exp2 = make_experiment(seq_id=2)
    task = RadiantGermlineAnnotationTask(
        task_id=10,
        part=1,
        analysis_type="germline",
        experiments=[exp1, exp2],
        vcf_filepath="/path/to/sample.vcf.gz",
    )
    assert task.task_id == 10
    assert len(task.experiments) == 2
    assert task.experiments[0].seq_id == 1
    assert task.experiments[1].seq_id == 2


def test_base_task_is_frozen():
    task = RadiantGermlineAnnotationTask(
        task_id=10,
        part=1,
        analysis_type="germline",
        experiments=[make_experiment()],
        vcf_filepath="/path/to/sample.vcf.gz",
    )
    with pytest.raises(ValidationError):
        task.part = 2


def test_radiant_germline_annotation_base_task():
    task = RadiantGermlineAnnotationTask(
        task_id=1,
        part=1,
        analysis_type="germline",
        experiments=[make_experiment()],
        vcf_filepath="/path/to/sample.vcf.gz",
    )
    assert task.vcf_filepath.endswith(".vcf.gz")
    assert task.index_vcf_filepath is None


def test_alignment_germline_variant_calling_task():
    task = AlignmentGermlineVariantCallingTask(
        task_id=2,
        part=1,
        analysis_type="germline",
        experiments=[make_experiment()],
        cnv_vcf_filepath="/path/to/cnv.vcf.gz",
    )
    assert "cnv" in task.cnv_vcf_filepath


def test_exomiser_base_task():
    task = ExomiserTask(
        task_id=3,
        part=1,
        analysis_type="germline",
        experiments=[make_experiment()],
        exomiser_filepath="/path/to/exomiser.yml",
    )
    assert "exomiser" in task.exomiser_filepath
