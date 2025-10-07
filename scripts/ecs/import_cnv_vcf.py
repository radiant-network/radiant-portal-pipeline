import argparse
import json
import logging
import os
import sys

from radiant.tasks.vcf.cnv.germline.process import import_cnv_vcf

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(cases: list[dict]):
    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    import_cnv_vcf(cases, namespace)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import CNV VCF for cases")
    parser.add_argument("--cases", required=True, help="Cases JSON string")
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    cases = json.loads(args.cases)

    main(cases)
