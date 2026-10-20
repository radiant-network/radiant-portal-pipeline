"""Command-line entry point shared by the ECS SNV writers (`scripts/ecs/import_*_snv_vcf_for_task.py`)."""

import argparse
import json
import logging
import os
import sys
from collections.abc import Callable

from radiant.tasks.utils import upload_json_to_s3

logger = logging.getLogger(__name__)


def run_writer(create_parquet_files: Callable[[dict, str], dict], description: str, argv: list[str] | None = None):
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--task", required=True, help="Task JSON string")
    parser.add_argument("--result_uri", default=None, help="S3 URI where the result JSON is written")
    args = parser.parse_args(argv)
    logger.info(f"Command line arguments: {args}")

    try:
        result = create_parquet_files(json.loads(args.task), os.environ["RADIANT_ICEBERG_NAMESPACE"])
        if args.result_uri:
            upload_json_to_s3(result, args.result_uri)
            logger.info(f"Result written to {args.result_uri}")
    except Exception as e:
        # Exiting non-zero is what makes the ECS task fail: without it a failed extraction is
        # marked successful and its partitions are silently missing from the commit.
        logger.exception(f"Error while processing task: {e}")
        sys.exit(1)
