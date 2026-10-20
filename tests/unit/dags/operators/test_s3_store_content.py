import json
from unittest.mock import patch

from radiant.dags.operators import utils


def test_s3_store_content_uploads_to_the_workspace_bucket(monkeypatch):
    monkeypatch.setattr(utils, "RADIANT_S3_WORKSPACE", "my-workspace")

    with patch("radiant.dags.operators.utils.boto3.client") as client:
        s3_path = utils.s3_store_content({"radiant.snv_variant": [{"id": 1}]}, prefix="commit_partitions")

    kwargs = client.return_value.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "my-workspace"
    assert kwargs["Key"].startswith("tmp/commit_partitions_") and kwargs["Key"].endswith(".json")
    assert json.loads(kwargs["Body"]) == {"radiant.snv_variant": [{"id": 1}]}
    assert s3_path == f"s3://my-workspace/{kwargs['Key']}"
