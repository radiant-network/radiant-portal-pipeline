import argparse
import json
import logging
import os
import sys

from radiant.tasks.utils import upload_json_to_s3
from radiant.tasks.vcf.snv.germline.process import create_parquet_files

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(task: dict, result_uri: str):
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    # `merge_commits` reads the result from `result_uri`, not from the operator's XCom: on ECS that
    # XCom is the last CloudWatch log event, read once right after the task stops, so it can be any
    # earlier log line when the last ones are not yet ingested.
    upload_json_to_s3(create_parquet_files(task, namespace), result_uri)
    logger.info(f"Result written to {result_uri}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import Germline SNV VCF for task")
    parser.add_argument("--task", required=True, help="Task JSON string")
    parser.add_argument("--result_uri", required=True, help="S3 URI where the result JSON is written")
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    task = json.loads(args.task)

    try:
        main(task, args.result_uri)
    except Exception as e:
        # Exiting non-zero is what makes the ECS task fail: without it a failed extraction is
        # marked successful and its partitions are silently missing from the commit.
        logger.exception(f"Error while processing task: {e}")
        sys.exit(1)
