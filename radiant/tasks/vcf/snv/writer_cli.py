"""Command-line entry point shared by the ECS SNV writers (`scripts/ecs/import_*_snv_vcf_for_task.py`)."""

import argparse
import json
import logging
import os
import sys
from collections.abc import Callable

from radiant.tasks.utils import upload_json_to_s3

logger = logging.getLogger(__name__)


def write_result(task: dict, result_uri: str | None, create_parquet_files: Callable[[dict, str], dict]) -> None:
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    result = create_parquet_files(task, namespace)
    # `merge_commits` reads the result from `result_uri`, not from the operator's XCom: on ECS that
    # XCom is the last CloudWatch log event, read once right after the task stops, so it can be any
    # earlier log line when the last ones are not yet ingested.
    if result_uri:
        upload_json_to_s3(result, result_uri)
        logger.info(f"Result written to {result_uri}")
    # TODO(RAD-92): remove once the operator image, plugins.zip and the DAGs all carry `--result_uri`.
    # Still printed, for a DAG that does not pass `--result_uri` yet and reads the XCom.
    print(json.dumps(result))


def run_writer(create_parquet_files: Callable[[dict, str], dict], description: str, argv: list[str] | None = None):
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--task", required=True, help="Task JSON string")
    parser.add_argument("--result_uri", default=None, help="S3 URI where the result JSON is written")
    args = parser.parse_args(argv)
    logger.info(f"Command line arguments: {args}")

    task = json.loads(args.task)

    try:
        write_result(task, args.result_uri, create_parquet_files)
    except Exception as e:
        # Exiting non-zero is what makes the ECS task fail: without it a failed extraction is
        # marked successful and its partitions are silently missing from the commit.
        logger.exception(f"Error while processing task: {e}")
        sys.exit(1)
