from unittest.mock import patch

import pytest

from radiant.tasks.vcf.snv import writer_cli
from radiant.tasks.vcf.snv.writer_cli import GermlineSnvWriter, SnvWriter, SomaticSnvWriter

RESULT = {"mock.snv_variant": [{"parquet_files": ["s3://mock/f.parquet"], "partition_filter": {}}]}


class MockWriter(SnvWriter):
    description = "mock"

    @staticmethod
    def create_parquet_files(task, namespace):
        assert namespace == "mock_ns"
        return RESULT


@pytest.fixture(autouse=True)
def _namespace(monkeypatch):
    monkeypatch.setenv("RADIANT_ICEBERG_NAMESPACE", "mock_ns")


def test_run_uploads_the_result():
    with patch.object(writer_cli, "upload_json_to_s3") as upload:
        MockWriter().run(["--task", "{}", "--result_uri", "s3://mock/r/0.json"])

    upload.assert_called_once_with(RESULT, "s3://mock/r/0.json")


def test_run_without_result_uri_fails_before_extracting():
    with (
        patch.object(MockWriter, "create_parquet_files") as create,
        pytest.raises(SystemExit) as exc_info,
    ):
        MockWriter().run(["--task", "{}"])

    assert exc_info.value.code == 2
    create.assert_not_called()


def test_run_exits_non_zero_when_the_upload_fails():
    with (
        patch.object(writer_cli, "upload_json_to_s3", side_effect=RuntimeError("denied")),
        pytest.raises(SystemExit) as exc_info,
    ):
        MockWriter().run(["--task", "{}", "--result_uri", "s3://mock/r/0.json"])
    assert exc_info.value.code == 1


@pytest.mark.parametrize(
    ("writer", "module"),
    [
        (GermlineSnvWriter, "radiant.tasks.vcf.snv.germline.process"),
        (SomaticSnvWriter, "radiant.tasks.vcf.snv.somatic.process"),
    ],
)
def test_writers_delegate_to_their_process_module(writer, module):
    with (
        patch(f"{module}.create_parquet_files", return_value=RESULT) as create,
        patch.object(writer_cli, "upload_json_to_s3") as upload,
    ):
        writer().run(["--task", '{"task_id": 1}', "--result_uri", "s3://mock/r/0.json"])

    create.assert_called_once_with({"task_id": 1}, "mock_ns")
    upload.assert_called_once_with(RESULT, "s3://mock/r/0.json")
