import argparse
import json
import logging
import os
import sys

from radiant.tasks.vcf.snv.germline.process import create_parquet_files

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(case: dict):
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    create_parquet_files(case, namespace)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import VCF for case")
    parser.add_argument("--case", required=True, help="Case JSON string")
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    case = json.loads(args.case)

    main(case)
