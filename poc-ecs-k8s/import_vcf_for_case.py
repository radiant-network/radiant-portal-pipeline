import json
import logging
import os
import sys
import tempfile
from urllib import parse

import boto3

from radiant.tasks.vcf.experiment import Case
from radiant.tasks.vcf.process import process_case

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def download_s3_file(s3_path, dest_dir):
    s3_client = boto3.client("s3")

    def extract_bucket_key(s3_path):
        parsed = parse.urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    bucket_name, object_key = extract_bucket_key(s3_path)
    local_path = os.path.join(dest_dir, os.path.basename(object_key))
    try:
        s3_client.download_file(bucket_name, object_key, local_path)
    except Exception as e:
        logger.error(f"Error downloading S3 file Bucket:[{bucket_name}], ObjectKey: [{object_key}] Filepath:[{local_path}]: {e}")
        raise Exception(f"Error downloading S3 file: {e}")

    return local_path


def merge_commits(partition_lists: dict[str, list[dict]]) -> dict[str, list[dict]]:
    from collections import defaultdict

    merged = defaultdict(list)
    for table, partitions in partition_lists.items():
        merged[table].extend(partitions)
    return dict(merged)


def commit_partitions(table_partitions: dict[str, list[dict]]):
    import logging
    import sys

    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)
    from pyiceberg.catalog import load_catalog

    from radiant.tasks.iceberg.partition_commit import PartitionCommit
    from radiant.tasks.iceberg.utils import commit_files

    catalog = load_catalog()
    for table_name, partitions in table_partitions.items():
        if not partitions:
            continue
        table = catalog.load_table(table_name)
        parts = [PartitionCommit.model_validate(pc) for pc in partitions]
        logger.info(f"🔁 Starting commit for table {table_name}")
        commit_files(table, parts)
        logger.info(f"✅ Changes commited to table {table_name}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import VCF for case")
    parser.add_argument("--case", required=True, help="Case JSON string")
    args = parser.parse_args()

    logger.info(f"Command line arguments: {args}")

    case = json.loads(args.case)

    logger.info("Downloading VCF and index files to a temporary directory")
    with tempfile.TemporaryDirectory() as tmpdir:
        vcf_local = download_s3_file(case["vcf_filepath"], tmpdir)
        index_local = download_s3_file(case["vcf_filepath"] + ".tbi", tmpdir)
        case["vcf_filepath"] = vcf_local
        case["index_vcf_filepath"] = index_local

        case = Case.model_validate(case)
        logger.info(f"🔁 STARTING IMPORT for Case: {case.case_id}")
        logger.info("=" * 80)

        namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE", "radiant")
        res = process_case(case, namespace=namespace, vcf_threads=4)
        logger.info(f"🔁 Parquet files created: {case.case_id}, file {case.vcf_filepath}")

        logger.info(f"🔁 Partition lists: {res}")

        logger.info("🔁 Merging commits from all tables")
        merged = merge_commits(res)

        logger.info("🔁 Merging completed, committing partitions")
        commit_partitions(merged)

        logger.info("✅ All done!")
