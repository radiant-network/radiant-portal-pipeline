import json
import logging
import uuid

import boto3

from radiant.dags import RADIANT_S3_WORKSPACE

logger = logging.getLogger(__name__)


def s3_store_content(content: dict | list, prefix: str = "tmp") -> str:
    s3_key = f"tmp/{prefix}_{uuid.uuid4().hex}.json"
    logger.info(f"Uploading content to S3: bucket={RADIANT_S3_WORKSPACE}, key={s3_key}")
    boto3.client("s3").put_object(Bucket=RADIANT_S3_WORKSPACE, Key=s3_key, Body=json.dumps(content).encode())
    return f"s3://{RADIANT_S3_WORKSPACE}/{s3_key}"


def _writer_results_prefix(dag_id: str, run_id: str) -> str:
    """Key prefix, in the workspace bucket, of the results of all the ECS writers of one DAG run."""
    return f"tmp/writer_results/{dag_id}/{run_id}/"


def _writer_result_key(dag_id: str, run_id: str, task_id: str, map_index: int | str) -> str:
    return f"{_writer_results_prefix(dag_id, run_id)}{task_id}/{map_index}.json"


def writer_result_s3_uri(dag_id: str, run_id: str, task_id: str, map_index: int | str) -> str:
    """S3 URI where one mapped ECS writer stores its result, and where `merge_commits` reads it.

    The key is deterministic, so both sides compute it without passing it through an XCom. The ECS
    operators call it with Jinja placeholders (``"{{ ti.run_id }}"``, ...) to build the command
    template; the reader calls it with the real values. A rerun of a writer overwrites its result.
    """
    return f"s3://{RADIANT_S3_WORKSPACE}/{_writer_result_key(dag_id, run_id, task_id, map_index)}"


class WriterResultError(Exception):
    """Raised when the result of one mapped writer is missing or is not a partition-commit JSON object."""


def parse_writer_result(result: dict | str | bytes | None, task_id: str, map_index: int) -> dict:
    """Return the result of the writer ``task_id[map_index]`` as a dict, or fail with an error naming it."""
    name = f"{task_id}[map_index={map_index}]"
    if isinstance(result, str | bytes):
        try:
            result = json.loads(result)
        # `ValueError`, not only `JSONDecodeError`: bytes that are not UTF-8 raise `UnicodeDecodeError`.
        except ValueError as e:
            raise WriterResultError(f"Result of {name} is not JSON ({e}), starts with: {result[:200]!r}") from e
    if not isinstance(result, dict):
        raise WriterResultError(f"Result of {name} is not a partition-commit object: {str(result)[:200]!r}")
    return result


def parse_writer_results(results: list | None, task_id: str) -> list[dict]:
    """Parse the results of all mapped instances of ``task_id``, in map-index order."""
    return [parse_writer_result(r, task_id, i) for i, r in enumerate(results or [])]


def s3_load_writer_results(dag_id: str, run_id: str, task_id: str, count: int) -> list[dict]:
    """Read the results of the ``count`` mapped instances of the ECS writer ``task_id`` from S3."""
    from botocore.exceptions import ClientError

    s3_client = boto3.client("s3")
    results = []
    for map_index in range(count):
        key = _writer_result_key(dag_id, run_id, task_id, map_index)
        try:
            body = s3_client.get_object(Bucket=RADIANT_S3_WORKSPACE, Key=key)["Body"].read()
        # The error code (`NoSuchKey` for a writer that stored nothing, `AccessDenied`, ...) is in `str(e)`.
        except ClientError as e:
            raise WriterResultError(
                f"Failed to read the result of {task_id}[map_index={map_index}] "
                f"at s3://{RADIANT_S3_WORKSPACE}/{key}: {e}"
            ) from e
        results.append(parse_writer_result(body, task_id, map_index))
    return results


def s3_delete_writer_results(dag_id: str, run_id: str) -> None:
    """Delete the results of all the ECS writers of one DAG run. Best effort: a failure only warns."""
    prefix = _writer_results_prefix(dag_id, run_id)
    s3_client = boto3.client("s3")
    try:
        for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=RADIANT_S3_WORKSPACE, Prefix=prefix):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                s3_client.delete_objects(Bucket=RADIANT_S3_WORKSPACE, Delete={"Objects": keys, "Quiet": True})
        logger.info(f"Deleted writer results under s3://{RADIANT_S3_WORKSPACE}/{prefix}")
    except Exception as e:
        logger.warning(f"Failed to delete writer results under s3://{RADIANT_S3_WORKSPACE}/{prefix}: {e}")


def s3_delete_content(s3_path: str) -> None:
    # Runs on the Airflow worker, which lacks the task image's dependencies: `radiant.tasks.utils`
    # (and its `delete_s3_object`) imports `wurlitzer`, so it can't be used here.
    bucket_name, key = s3_path[len("s3://") :].split("/", 1)
    try:
        boto3.client("s3").delete_object(Bucket=bucket_name, Key=key)
        logger.info(f"Deleted S3 content: {s3_path}")
    except Exception as e:
        logger.warning(f"Failed to delete S3 content {s3_path}: {e}")
