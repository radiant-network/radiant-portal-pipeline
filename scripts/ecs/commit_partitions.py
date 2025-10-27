import argparse
import json
import logging
import sys

import boto3

from radiant.tasks.vcf.snv.germline.process import commit_partitions


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


def download_json_from_s3(s3_path: str, local_path: str, logger) -> dict:
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    s3_path_trimmed = s3_path[len("s3://") :]
    bucket_name, key = s3_path_trimmed.split("/", 1)

    logger.info(f"Downloading JSON from S3: bucket={bucket_name}, key={key} to local_path={local_path}")

    s3_client = boto3.client("s3")
    s3_client.download_file(bucket_name, key, local_path)

    with open(local_path) as f:
        return json.load(f)


def delete_s3_object(s3_path: str, logger):
    try:
        s3_path_trimmed = s3_path[len("s3://") :]
        bucket_name, key = s3_path_trimmed.split("/", 1)
        s3_client = boto3.client("s3")
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        logger.info(f"Deleted temporary S3 file: s3://{bucket_name}/{key}")
    except Exception as e:
        logger.warning(f"Failed to delete temporary S3 file: {e}")


def main():
    logger = setup_logging()

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
        partitions = download_json_from_s3(args.table_partitions, local_tmp_path, logger)
        commit_partitions(partitions)
    except Exception as e:
        logger.error(f"Error while processing partitions: {e}")
        sys.exit(1)
    finally:
        delete_s3_object(args.table_partitions, logger)


if __name__ == "__main__":
    main()
