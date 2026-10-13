import json
from unittest.mock import patch

from radiant.dags.operators import utils


def test_s3_store_content_uploads_to_the_workspace_bucket(monkeypatch):
    monkeypatch.setattr(utils, "RADIANT_S3_WORKSPACE", "my-workspace")
    uploaded = {}

    def fake_upload(local_path, bucket, key):
        with open(local_path) as f:
            uploaded.update(bucket=bucket, key=key, content=json.load(f))

    with patch("radiant.dags.operators.utils.boto3.client") as client:
        client.return_value.upload_file.side_effect = fake_upload
        s3_path = utils.s3_store_content({"radiant.snv_variant": [{"id": 1}]}, prefix="commit_partitions")

    assert uploaded["bucket"] == "my-workspace"
    assert uploaded["key"].startswith("tmp/commit_partitions_") and uploaded["key"].endswith(".json")
    assert uploaded["content"] == {"radiant.snv_variant": [{"id": 1}]}
    assert s3_path == f"s3://my-workspace/{uploaded['key']}"
