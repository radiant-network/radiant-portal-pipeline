import argparse
import logging
import os
import sys

from radiant.tasks.utils import download_json_from_s3, delete_s3_object
from radiant.tasks.vcf.cnv.germline.process import import_cnv_vcf

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(cases: list[dict]):
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    import_cnv_vcf(cases, namespace)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cases from an S3 JSON file")

    parser.add_argument(
        "--cases",
        required=True,
        help="S3 path to a JSON file containing the table partitions",
    )
    args = parser.parse_args()
    logger.info(f"Received argument --cases={args.cases}")

    local_tmp_path = "/tmp/cases.json"

    try:
        cases = download_json_from_s3(args.cases, local_tmp_path)
        logger.info(f"Downloaded cases: {cases}")
        main(cases)
    except Exception as e:
        logger.error(f"Error while processing cases: {e}")
        sys.exit(1)
    finally:
        delete_s3_object(args.cases, logger)
