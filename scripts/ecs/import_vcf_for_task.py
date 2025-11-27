import argparse
import json
import logging
import os
import sys

from radiant.tasks.vcf.snv.germline.process import create_parquet_files

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(task: dict):
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    print(json.dumps(create_parquet_files(task, namespace)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import VCF for task")
    parser.add_argument("--task", required=True, help="Task JSON string")
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    task = json.loads(args.task)

    main(task)
