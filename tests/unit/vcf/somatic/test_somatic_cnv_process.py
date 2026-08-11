from unittest.mock import patch

import pytest

from radiant.tasks.utils import S3DownloadError
from radiant.tasks.vcf.cnv.somatic import process as cnv_process
from radiant.tasks.vcf.experiment import (
    ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK,
    TUMOR_ONLY_VARIANT_CALLING_TASK,
    Experiment,
    TumorOnlyVariantCallingTask,
)
from tests.unit.vcf.vcf_test_utils import RESOURCES_DIR


def _experiment(seq_id: int = 64, aliquot: str = "TCRBOA6-T", histology_type: str = "tumoral") -> Experiment:
    return Experiment(
        seq_id=seq_id,
        patient_id=62,
        aliquot=aliquot,
        tenant_code="tenant1",
        family_role="proband",
        affected_status="affected",
        sex="female",
        experimental_strategy="wgs",
        request_priority="routine",
        histology_type=histology_type,
    )


def _task(experiments: list[Experiment] | None = None) -> TumorOnlyVariantCallingTask:
    return TumorOnlyVariantCallingTask(
        task_id=70,
        part=0,
        analysis_type="somatic",
        deleted=False,
        experiments=experiments if experiments is not None else [_experiment()],
        cnv_vcf_filepath="/tmp/tumor_only.cnv.vcf.gz",
    )


def _task_dict(task_id: int, filepath: str | None, task_type: str = TUMOR_ONLY_VARIANT_CALLING_TASK) -> dict:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "cnv_vcf_filepath": filepath,
    }


def test_validate_task_is_tumor_only_accepts_single_tumoral_sample():
    cnv_process.validate_task_is_tumor_only(_task(), ["TCRBOA6-T"])


def test_validate_task_is_tumor_only_rejects_two_experiments():
    """Two aliquots on the task means a tumor-normal analysis mislabelled upstream."""
    task = _task([_experiment(), _experiment(seq_id=65, aliquot="TCRBOA6-N", histology_type="normal")])

    with pytest.raises(ValueError, match="2 experiments"):
        cnv_process.validate_task_is_tumor_only(task, ["TCRBOA6-T"])


def test_validate_task_is_tumor_only_rejects_non_tumoral_experiment():
    """A normal-only task would otherwise write normal depths as tumor values."""
    task = _task([_experiment(histology_type="normal")])

    with pytest.raises(ValueError, match="histology_type 'normal'"):
        cnv_process.validate_task_is_tumor_only(task, ["TCRBOA6-T"])


def test_validate_task_is_tumor_only_rejects_multi_sample_vcf():
    """The only check that catches a genuinely tumor-normal *file*: the task looks tumor-only because
    its normal experiment never registered upstream, but the VCF still carries both samples."""
    with pytest.raises(ValueError, match="declares 2 samples"):
        cnv_process.validate_task_is_tumor_only(_task(), ["TCRBOA6-T", "TCRBOA6-N"])


def test_process_tasks_refuses_to_overwrite_with_an_empty_buffer():
    """No tasks must not wipe the table, and must not need a catalog to decide that."""
    with patch.object(cnv_process, "load_catalog") as mock_load_catalog:
        res = cnv_process.process_tasks([], namespace="radiant")

    mock_load_catalog.assert_not_called()
    assert res == {"radiant.somatic_cnv_occurrence": []}


def test_process_tasks_buffers_only_classifiable_records():
    """The reference segment (ALT `.`) and the `<CNV>` segment must never reach the table: both would
    produce a NULL `cnv_id` against a NOT NULL key column and fail the whole StarRocks load."""
    task = _task()
    task = task.model_copy(update={"cnv_vcf_filepath": str(RESOURCES_DIR / "test_somatic_cnv.vcf")})

    with (
        patch.object(cnv_process, "load_catalog"),
        patch.object(cnv_process, "pa") as mock_pa,
    ):
        cnv_process.process_tasks([task], namespace="radiant")

    buffered = mock_pa.Table.from_pylist.call_args.args[0]
    assert [(occ["type"], occ["alternate"]) for occ in buffered] == [
        ("LOSS", "<DEL>"),
        ("CNLOH", "<LOH>"),
        ("GAIN", "<DUP>"),
        ("GAINLOH", "<LOH>"),
        ("LOSS", "<DEL>"),
    ]
    assert {occ["seq_id"] for occ in buffered} == {64}
    assert {occ["tenant_code"] for occ in buffered} == {"tenant1"}
    assert {occ["task_id"] for occ in buffered} == {70}


def test_process_tasks_rejects_a_two_sample_vcf():
    """The multi-sample check has to see the *file's* sample list, which only exists before
    `set_samples` narrows it -- narrowing rewrites `vcf.samples` and `vcf.raw_header` and destroys the
    evidence. Exercised through `process_tasks` rather than `validate_task_is_tumor_only` directly,
    because it is that ordering, not the validation itself, that a refactor would quietly break."""
    task = _task().model_copy(update={"cnv_vcf_filepath": str(RESOURCES_DIR / "test_somatic_cnv_two_samples.vcf")})

    with (
        patch.object(cnv_process, "load_catalog"),
        patch.object(cnv_process, "pa") as mock_pa,
        pytest.raises(ValueError, match="declares 2 samples"),
    ):
        cnv_process.process_tasks([task], namespace="radiant")

    # Tumor-normal CNV is out of scope: fail loudly rather than half-succeed on the tumor sample.
    mock_pa.Table.from_pylist.assert_not_called()


def test_import_somatic_cnv_vcf_processes_only_tumor_only_tasks():
    tasks = [
        _task_dict(1, "s3://bucket/germline.cnv.vcf.gz", task_type=ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK),
        _task_dict(2, "s3://bucket/tumor_only.cnv.vcf.gz"),
    ]

    with (
        patch.object(cnv_process, "download_s3_file", return_value="/tmp/local.cnv.vcf.gz"),
        patch.object(cnv_process.TumorOnlyVariantCallingTask, "model_validate", side_effect=lambda t: t),
        patch.object(cnv_process, "process_tasks") as mock_process,
    ):
        cnv_process.import_somatic_cnv_vcf(tasks, namespace="radiant")

    processed = mock_process.call_args.args[0]
    assert [t["task_id"] for t in processed] == [2]
    assert processed[0]["cnv_vcf_filepath"] == "/tmp/local.cnv.vcf.gz"


def test_import_somatic_cnv_vcf_skips_task_without_filepath():
    """`cnv_vcf_filepath` is nullable on the model, and a missing URL is nothing to extract -- not a
    reason to abort the other tasks of the part."""
    tasks = [_task_dict(1, None), _task_dict(2, "s3://bucket/tumor_only.cnv.vcf.gz")]

    with (
        patch.object(cnv_process, "download_s3_file", return_value="/tmp/local.cnv.vcf.gz"),
        patch.object(cnv_process.TumorOnlyVariantCallingTask, "model_validate", side_effect=lambda t: t),
        patch.object(cnv_process, "process_tasks") as mock_process,
    ):
        cnv_process.import_somatic_cnv_vcf(tasks, namespace="radiant")

    processed = mock_process.call_args.args[0]
    assert [t["task_id"] for t in processed] == [2]


def test_import_somatic_cnv_vcf_propagates_download_failure():
    """Unlike germline CNV (SJRA-1784), a failed download must not be swallowed: the task would
    succeed, the experiment would be marked ingested, and the delta would never offer it again."""
    tasks = [_task_dict(1, "s3://bucket/bad.cnv.vcf.gz"), _task_dict(2, "s3://bucket/good.cnv.vcf.gz")]

    def fake_download(s3_path, _tmpdir, randomize_filename=False):
        if s3_path == "s3://bucket/bad.cnv.vcf.gz":
            raise S3DownloadError(f"Failed to download S3 file {s3_path}")
        return "/tmp/good.cnv.vcf.gz"

    with (
        patch.object(cnv_process, "download_s3_file", side_effect=fake_download),
        patch.object(cnv_process.TumorOnlyVariantCallingTask, "model_validate", side_effect=lambda t: t),
        patch.object(cnv_process, "process_tasks") as mock_process,
        pytest.raises(S3DownloadError),
    ):
        cnv_process.import_somatic_cnv_vcf(tasks, namespace="radiant")

    mock_process.assert_not_called()
