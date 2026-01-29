import argparse
import logging
import os
import sys

from radiant.tasks.utils import delete_s3_object, download_json_from_s3
from radiant.tasks.vcf.cnv.germline.process import import_cnv_vcf

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(tasks: list[dict]):
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    import_cnv_vcf(tasks, namespace)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tasks from an S3 JSON file")

    parser.add_argument(
        "--tasks",
        required=True,
        help="S3 path to a JSON file containing the table partitions",
    )
    args = parser.parse_args()
    logger.info(f"Received argument --tasks={args.tasks}")

    local_tmp_path = "/tmp/tasks.json"

    try:
        tasks = download_json_from_s3(args.tasks, local_tmp_path, logger)
        logger.info(f"Downloaded tasks: {tasks}")
        main(tasks)
    except Exception as e:
        logger.exception(f"Error while processing tasks: {e}")
        sys.exit(1)
    finally:
        delete_s3_object(args.tasks, logger)
