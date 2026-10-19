import json
from unittest.mock import patch

import pytest

from radiant.tasks.vcf.snv import writer_cli

RESULT = {"mock.snv_variant": [{"parquet_files": ["s3://mock/f.parquet"], "partition_filter": {}}]}


def _create(task, namespace):
    assert namespace == "mock_ns"
    return RESULT


@pytest.fixture(autouse=True)
def _namespace(monkeypatch):
    monkeypatch.setenv("RADIANT_ICEBERG_NAMESPACE", "mock_ns")


def test_run_writer_uploads_the_result_and_still_prints_it(capsys):
    with patch.object(writer_cli, "upload_json_to_s3") as upload:
        writer_cli.run_writer(_create, "mock", ["--task", "{}", "--result_uri", "s3://mock/r/0.json"])

    upload.assert_called_once_with(RESULT, "s3://mock/r/0.json")
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == RESULT


def test_run_writer_without_result_uri_only_prints(capsys):
    with patch.object(writer_cli, "upload_json_to_s3") as upload:
        writer_cli.run_writer(_create, "mock", ["--task", "{}"])

    upload.assert_not_called()
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == RESULT


def test_run_writer_exits_non_zero_when_the_upload_fails():
    with (
        patch.object(writer_cli, "upload_json_to_s3", side_effect=RuntimeError("denied")),
        pytest.raises(SystemExit) as exc_info,
    ):
        writer_cli.run_writer(_create, "mock", ["--task", "{}", "--result_uri", "s3://mock/r/0.json"])
    assert exc_info.value.code == 1
