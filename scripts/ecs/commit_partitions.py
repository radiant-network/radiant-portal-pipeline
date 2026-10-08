import argparse
import logging
import sys

from radiant.tasks.iceberg.utils import commit_partitions_from_s3

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Commit Table Partitions from an S3 JSON file")
    parser.add_argument(
        "--table_partitions",
        required=True,
        help="S3 path to a JSON file containing the table partitions",
    )
    args = parser.parse_args()
    logger.info(f"Received argument --table_partitions={args.table_partitions}")

    try:
        commit_partitions_from_s3(args.table_partitions)
    except Exception as e:
        logger.exception(f"Error while processing partitions: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
