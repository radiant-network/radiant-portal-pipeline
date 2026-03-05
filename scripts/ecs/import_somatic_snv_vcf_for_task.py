import argparse
import json
import logging
import os
import sys

from radiant.tasks.vcf.snv.somatic.process import create_parquet_files

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(tasks: list[dict]):
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    print(json.dumps(create_parquet_files(tasks, namespace)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import Somatic SNV VCF for task")
    parser.add_argument("--tasks", required=True, help="Tasks JSON string")
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    tasks = json.loads(args.tasks)

    try:
        main(tasks)
    except Exception as e:
        logger.exception(f"Error while processing task: {e}")
