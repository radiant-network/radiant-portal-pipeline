import argparse
import logging
import sys


from radiant.tasks.utils import download_json_from_s3, delete_s3_object
from radiant.tasks.vcf.snv.germline.process import commit_partitions

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

    local_tmp_path = "/tmp/table_partitions.json"

    try:
        partitions = download_json_from_s3(args.table_partitions, local_tmp_path)
        commit_partitions(partitions)
    except Exception as e:
        logger.error(f"Error while processing partitions: {e}")
        sys.exit(1)
    finally:
        delete_s3_object(args.table_partitions, logger)


if __name__ == "__main__":
    main()
