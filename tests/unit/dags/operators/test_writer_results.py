import io
import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import jinja2
import pytest
from botocore.exceptions import ClientError

from radiant.dags.operators import utils
from radiant.dags.operators.utils import (
    WriterResultError,
    parse_writer_result,
    parse_writer_results,
    s3_delete_writer_results,
    s3_load_writer_results,
    writer_result_s3_uri,
)

TASK_ID = "create_germline_parquet_files_ecs"
RESULT = {"mock.snv_variant": [{"partition": {"part": 1}, "parquet_paths": ["s3://mock/f.parquet"]}]}


def test_parse_writer_result_names_the_map_index_of_a_non_json_result():
    # The XCom that blocked a commit: the last CloudWatch line read before the result was ingested.
    log_line = "INFO:radiant.tasks.vcf.snv.germline.process:" + "=" * 80
    with pytest.raises(WriterResultError, match=rf"{TASK_ID}\[map_index=3\] is not JSON"):
        parse_writer_result(log_line, TASK_ID, 3)


def test_parse_writer_result_names_the_map_index_of_non_utf8_bytes():
    with pytest.raises(WriterResultError, match=rf"{TASK_ID}\[map_index=2\] is not JSON"):
        parse_writer_result(b"\xff\xfe", TASK_ID, 2)


@pytest.mark.parametrize("result", [None, "[1, 2]", 42])
def test_parse_writer_result_rejects_a_non_object(result):
    with pytest.raises(WriterResultError, match=rf"{TASK_ID}\[map_index=0\]"):
        parse_writer_result(result, TASK_ID, 0)


@pytest.mark.parametrize("result", [RESULT, json.dumps(RESULT), json.dumps(RESULT).encode()])
def test_parse_writer_result_accepts_dict_str_and_bytes(result):
    assert parse_writer_result(result, TASK_ID, 0) == RESULT


def test_parse_writer_results_reports_the_failing_index():
    with pytest.raises(WriterResultError, match=r"\[map_index=1\]"):
        parse_writer_results([RESULT, "not json"], TASK_ID)
    assert parse_writer_results(None, TASK_ID) == []


def test_writer_result_template_renders_to_the_reader_uri(monkeypatch):
    """The ECS command template and the reader must agree on the key, or merge_commits finds nothing."""
    monkeypatch.setattr(utils, "RADIANT_S3_WORKSPACE", "mock-workspace")
    template = writer_result_s3_uri("{{ ti.dag_id }}", "{{ ti.run_id }}", "{{ ti.task_id }}", "{{ ti.map_index }}")
    ti = SimpleNamespace(dag_id="mock-dag", run_id="manual__2026-01-01T00:00:00+00:00", task_id=TASK_ID, map_index=7)

    rendered = jinja2.Template(template).render(ti=ti)

    assert rendered == writer_result_s3_uri(ti.dag_id, ti.run_id, ti.task_id, 7)
    assert rendered.startswith("s3://mock-workspace/tmp/writer_results/mock-dag/")
    assert rendered.endswith(f"/{TASK_ID}/7.json")


def test_s3_load_writer_results_reads_one_object_per_map_index(monkeypatch):
    monkeypatch.setattr(utils, "RADIANT_S3_WORKSPACE", "mock-workspace")
    with patch("radiant.dags.operators.utils.boto3.client") as client:
        client.return_value.get_object.side_effect = lambda Bucket, Key: {
            "Body": io.BytesIO(json.dumps({"key": Key}).encode())
        }
        results = s3_load_writer_results("mock-dag", "mock-run", TASK_ID, 2)

    assert results == [
        {"key": f"tmp/writer_results/mock-dag/mock-run/{TASK_ID}/0.json"},
        {"key": f"tmp/writer_results/mock-dag/mock-run/{TASK_ID}/1.json"},
    ]
    assert client.return_value.get_object.call_args_list[0].kwargs["Bucket"] == "mock-workspace"


def test_s3_load_writer_results_names_the_map_index_of_a_missing_result():
    missing = ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
    with patch("radiant.dags.operators.utils.boto3.client") as client:
        client.return_value.get_object.side_effect = [{"Body": io.BytesIO(json.dumps(RESULT).encode())}, missing]
        with pytest.raises(WriterResultError, match=rf"{TASK_ID}\[map_index=1\].*NoSuchKey"):
            s3_load_writer_results("mock-dag", "mock-run", TASK_ID, 2)


def test_s3_load_writer_results_names_the_error_code_of_a_read_failure():
    denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "GetObject")
    with patch("radiant.dags.operators.utils.boto3.client") as client:
        client.return_value.get_object.side_effect = denied
        with pytest.raises(
            WriterResultError, match=rf"Failed to read the result of {TASK_ID}\[map_index=0\].*AccessDenied"
        ):
            s3_load_writer_results("mock-dag", "mock-run", TASK_ID, 1)


def test_s3_load_writer_results_reads_nothing_for_zero_writers():
    with patch("radiant.dags.operators.utils.boto3.client") as client:
        assert s3_load_writer_results("mock-dag", "mock-run", TASK_ID, 0) == []
    client.return_value.get_object.assert_not_called()


@pytest.mark.parametrize("kind", ["germline", "somatic"])
def test_ecs_writer_command_passes_the_result_uri(kind):
    from radiant.dags.operators.ecs import ImportSNVVCF

    ecs_env = SimpleNamespace(ECS_CLUSTER="mock-cluster", ECS_SUBNETS=["subnet-1"], ECS_SECURITY_GROUPS=["sg-1"])
    with patch.dict(os.environ, {"RADIANT_ENVIRONMENT": "mock"}):
        op = getattr(ImportSNVVCF, f"get_create_{kind}_parquet_files")("ns", ecs_env)
    op._expand_called = True

    (command,) = op.kwargs["overrides"]["containerOverrides"][0]["command"]
    assert "--result_uri '" in command
    assert "{{ ti.run_id }}/{{ ti.task_id }}/{{ ti.map_index }}.json" in command


def test_s3_delete_writer_results_deletes_every_page_under_the_run_prefix(monkeypatch):
    monkeypatch.setattr(utils, "RADIANT_S3_WORKSPACE", "mock-workspace")
    pages = [{"Contents": [{"Key": "a"}, {"Key": "b"}]}, {}, {"Contents": [{"Key": "c"}]}]
    with patch("radiant.dags.operators.utils.boto3.client") as client:
        client.return_value.get_paginator.return_value.paginate.return_value = pages
        s3_delete_writer_results("mock-dag", "mock-run")

    client.return_value.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="mock-workspace", Prefix="tmp/writer_results/mock-dag/mock-run/"
    )
    deleted = [c.kwargs["Delete"]["Objects"] for c in client.return_value.delete_objects.call_args_list]
    assert deleted == [[{"Key": "a"}, {"Key": "b"}], [{"Key": "c"}]]


def test_s3_delete_writer_results_only_warns_on_failure(caplog):
    with patch("radiant.dags.operators.utils.boto3.client") as client:
        client.return_value.get_paginator.side_effect = RuntimeError("boom")
        s3_delete_writer_results("mock-dag", "mock-run")
    assert "Failed to delete writer results" in caplog.text
