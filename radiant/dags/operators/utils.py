import json
import logging
import os
import tempfile

import boto3

from radiant.dags import RADIANT_S3_WORKSPACE

logger = logging.getLogger(__name__)


def s3_store_content(content: dict | list, prefix: str = "tmp") -> str:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
        json.dump(content, tmpfile)
        tmpfile_path = tmpfile.name

    s3_client = boto3.client("s3")
    bucket_name = RADIANT_S3_WORKSPACE
    s3_key = f"tmp/{prefix}_{os.path.basename(tmpfile_path)}"

    logger.info(f"Uploading content to S3: bucket={bucket_name}, key={s3_key}")
    s3_client.upload_file(tmpfile_path, bucket_name, s3_key)
    s3_path = f"s3://{bucket_name}/{s3_key}"

    os.remove(tmpfile_path)
    return s3_path


def s3_delete_content(s3_path: str) -> None:
    # Runs on the Airflow worker, which lacks the task image's dependencies: `radiant.tasks.utils`
    # (and its `delete_s3_object`) imports `wurlitzer`, so it can't be used here.
    bucket_name, key = s3_path[len("s3://") :].split("/", 1)
    try:
        boto3.client("s3").delete_object(Bucket=bucket_name, Key=key)
        logger.info(f"Deleted S3 content: {s3_path}")
    except Exception as e:
        logger.warning(f"Failed to delete S3 content {s3_path}: {e}")
