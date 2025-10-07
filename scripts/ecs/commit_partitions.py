import argparse
import json
import logging
import sys

from radiant.tasks.vcf.snv.germline.process import commit_partitions

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(table_partitions: dict[str, list[dict]]):
    commit_partitions(table_partitions)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Commmit Table Partitions")
    parser.add_argument("--table_partitions", required=True, help="Table Partitions JSON string")
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    table_partitions = json.loads(args.table_partitions)

    main(table_partitions)
