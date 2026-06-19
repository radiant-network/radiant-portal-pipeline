from unittest.mock import patch

import pytest

from radiant.tasks.utils import S3DownloadError
from radiant.tasks.vcf.cnv.germline import process as cnv_process
from radiant.tasks.vcf.experiment import ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK


def _task(task_id: int, filepath: str) -> dict:
    return {
        "task_id": task_id,
        "task_type": ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK,
        "cnv_vcf_filepath": filepath,
    }


def test_import_cnv_vcf_skips_task_when_not_all_download_fails():
    """A least one (but not all) failed S3 download must not crash the whole batch."""
    tasks = [_task(1, "s3://bucket/bad.vcf.gz"), _task(2, "s3://bucket/good.vcf.gz")]

    def fake_download(s3_path, _tmpdir, randomize_filename=False):
        if s3_path == "s3://bucket/bad.vcf.gz":
            raise S3DownloadError("Failed to download S3 file s3://bucket/bad.vcf.gz")
        return "/tmp/good.vcf.gz"

    with (
        patch.object(cnv_process, "download_s3_file", side_effect=fake_download),
        patch.object(cnv_process.AlignmentGermlineVariantCallingTask, "model_validate", side_effect=lambda t: t),
        patch.object(cnv_process, "process_tasks") as mock_process,
    ):
        cnv_process.import_cnv_vcf(tasks, namespace="radiant")

    # Only the good task reaches processing; the failed download is skipped, not fatal.
    mock_process.assert_called_once()
    processed = mock_process.call_args.args[0]
    assert [t["task_id"] for t in processed] == [2]
    assert processed[0]["cnv_vcf_filepath"] == "/tmp/good.vcf.gz"


def test_import_cnv_vcf_raises_when_all_downloads_fail():
    """When all downloads fail, we must stop the whole batch."""
    tasks = [_task(1, "s3://bucket/a.vcf.gz"), _task(2, "s3://bucket/b.vcf.gz")]

    def fake_download(s3_path, _tmpdir, randomize_filename=False):
        raise S3DownloadError(f"Failed to download S3 file {s3_path}")

    with (
        patch.object(cnv_process, "download_s3_file", side_effect=fake_download),
        patch.object(cnv_process.AlignmentGermlineVariantCallingTask, "model_validate", side_effect=lambda t: t),
        patch.object(cnv_process, "process_tasks") as mock_process,
        pytest.raises(RuntimeError, match=r"download\(s\) failed"),
    ):
        cnv_process.import_cnv_vcf(tasks, namespace="radiant")

    mock_process.assert_not_called()


def test_import_cnv_vcf_processes_all_when_downloads_succeed():
    tasks = [_task(1, "s3://bucket/a.vcf.gz"), _task(2, "s3://bucket/b.vcf.gz")]

    def fake_download(s3_path, _tmpdir, randomize_filename=False):
        return f"/tmp/{s3_path[-8:]}"

    with (
        patch.object(cnv_process, "download_s3_file", side_effect=fake_download),
        patch.object(cnv_process.AlignmentGermlineVariantCallingTask, "model_validate", side_effect=lambda t: t),
        patch.object(cnv_process, "process_tasks") as mock_process,
    ):
        cnv_process.import_cnv_vcf(tasks, namespace="radiant")

    processed = mock_process.call_args.args[0]
    assert [t["task_id"] for t in processed] == [1, 2]
