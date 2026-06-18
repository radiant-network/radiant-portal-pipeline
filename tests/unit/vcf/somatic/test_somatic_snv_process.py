from unittest.mock import MagicMock, patch

import pytest

from radiant.tasks.utils import S3DownloadError
from radiant.tasks.vcf.experiment import RADIANT_SOMATIC_ANNOTATION_TASK, Experiment
from radiant.tasks.vcf.snv.somatic import process as somatic_process
from radiant.tasks.vcf.snv.somatic.process import get_somatic_indexes


def make_experiment(aliquot: str, histology_type: str) -> Experiment:
    exp = MagicMock(spec=Experiment)
    exp.aliquot = aliquot
    exp.histology_type = histology_type
    return exp


def test_tumor_first_normal_second():
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    samples = ["SAMPLE_T", "SAMPLE_N"]
    tumor_index, normal_index = get_somatic_indexes(experiments, samples)
    assert tumor_index == 0
    assert normal_index == 1


def test_normal_first_tumor_second():
    """Column order in VCF may differ from experiment list order."""
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    samples = ["SAMPLE_N", "SAMPLE_T"]  # VCF has normal first
    tumor_index, normal_index = get_somatic_indexes(experiments, samples)
    assert tumor_index == 1
    assert normal_index == 0


def test_experiment_list_order_does_not_affect_result():
    """Indexes should reflect VCF column positions, not experiment list order."""
    experiments_a = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    experiments_b = [
        make_experiment("SAMPLE_N", "normal"),
        make_experiment("SAMPLE_T", "tumoral"),
    ]
    samples = ["SAMPLE_N", "SAMPLE_T"]
    assert get_somatic_indexes(experiments_a, samples) == get_somatic_indexes(experiments_b, samples)


def test_missing_tumor_raises():
    experiments = [make_experiment("SAMPLE_N", "normal")]
    with pytest.raises(ValueError, match="tumor"):
        get_somatic_indexes(experiments, ["SAMPLE_N"])


def test_missing_normal_raises():
    experiments = [make_experiment("SAMPLE_T", "tumoral")]
    with pytest.raises(ValueError, match="normal"):
        get_somatic_indexes(experiments, ["SAMPLE_T"])


def test_empty_experiments_raises():
    with pytest.raises(ValueError):
        get_somatic_indexes([], ["SAMPLE_T", "SAMPLE_N"])


def test_aliquot_not_in_samples_raises():
    """Experiment aliquot that isn't present in VCF samples should raise."""
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    with pytest.raises(ValueError):
        get_somatic_indexes(experiments, ["SAMPLE_T", "SAMPLE_WRONG"])


def test_unknown_histology_type_ignored():
    """Extra experiments with unrecognised histology_type should not affect result."""
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
        make_experiment("SAMPLE_X", "metastasis"),  # unknown type
    ]
    samples = ["SAMPLE_N", "SAMPLE_T", "SAMPLE_X"]
    tumor_index, normal_index = get_somatic_indexes(experiments, samples)
    assert tumor_index == 1
    assert normal_index == 0


def test_returns_tuple_of_two_ints():
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    result = get_somatic_indexes(experiments, ["SAMPLE_N", "SAMPLE_T"])
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(i, int) for i in result)


def _somatic_task(task_id: int, filepath: str) -> dict:
    return {
        "task_id": task_id,
        "task_type": RADIANT_SOMATIC_ANNOTATION_TASK,
        "vcf_filepath": filepath,
    }


def test_import_somatic_snv_skips_task_when_download_fails():
    """A single failed S3 download must not abort the whole somatic batch."""
    tasks = [_somatic_task(1, "s3://bucket/bad.vcf.gz"), _somatic_task(2, "s3://bucket/good.vcf.gz")]

    def fake_download(s3_path, _tmpdir, randomize_filename=False):
        if s3_path.startswith("s3://bucket/bad"):
            raise S3DownloadError("Failed to download S3 file s3://bucket/bad.vcf.gz")
        return f"/tmp/{s3_path.rsplit('/', 1)[-1]}"

    with (
        patch.object(somatic_process, "download_s3_file", side_effect=fake_download),
        patch.object(somatic_process.RadiantSomaticAnnotationTask, "model_validate", side_effect=lambda d: d),
        patch.object(somatic_process, "process_task", return_value={"radiant.snv": [{"id": "x"}]}) as mock_proc,
        patch.object(somatic_process, "commit_partitions") as mock_commit,
    ):
        somatic_process.import_somatic_snv(tasks, namespace="radiant")

    # Only the good task is processed; the failed download is skipped, not fatal.
    assert mock_proc.call_count == 1
    processed_task = mock_proc.call_args.args[0]
    assert processed_task["task_id"] == 2
    # The batch still commits the good task's partitions exactly once.
    mock_commit.assert_called_once()
    assert dict(mock_commit.call_args.args[0]) == {"radiant.snv": [{"id": "x"}]}


def test_import_somatic_snv_raises_when_all_downloads_fail():
    """A total S3 outage must fail the batch (so Airflow retries), not report success."""
    tasks = [_somatic_task(1, "s3://bucket/a.vcf.gz"), _somatic_task(2, "s3://bucket/b.vcf.gz")]

    def fake_download(s3_path, _tmpdir, randomize_filename=False):
        raise S3DownloadError(f"Failed to download S3 file {s3_path}")

    with (
        patch.object(somatic_process, "download_s3_file", side_effect=fake_download),
        patch.object(somatic_process.RadiantSomaticAnnotationTask, "model_validate", side_effect=lambda d: d),
        patch.object(somatic_process, "process_task") as mock_proc,
        patch.object(somatic_process, "commit_partitions") as mock_commit,
        pytest.raises(RuntimeError, match=r"download\(s\) failed"),
    ):
        somatic_process.import_somatic_snv(tasks, namespace="radiant")

    # Nothing processed and nothing committed — the run fails instead of silently succeeding.
    mock_proc.assert_not_called()
    mock_commit.assert_not_called()
