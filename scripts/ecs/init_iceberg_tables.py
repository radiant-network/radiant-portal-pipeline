import argparse
import json
import logging
import sys

from radiant.tasks.iceberg import initialization

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(to_initialize: str):
    if to_initialize == "database":
        initialization.init_database()
    elif to_initialize == "germline_snv_occurrence":
        initialization.create_germline_snv_occurrence_table()
    elif to_initialize == "germline_variant":
        initialization.create_germline_variant_table()
    elif to_initialize == "germline_consequence":
        initialization.create_germline_consequences_table()
    elif to_initialize == "germline_cnv_occurrence":
        initialization.create_germline_cnv_occurrence_table()
    else:
        raise ValueError(
            f"Unknown initialization name: {to_initialize}, possible values are: "
            "'database', 'germline_snv_occurrence', 'germline_variant', "
            "'germline_consequence', 'germline_cnv_occurrence'"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize Iceberg")
    parser.add_argument(
        "--name",
        required=True,
        help="Iceberg target to initialize, possible values are: "
        "'database', 'germline_snv_occurrence', 'germline_variant', "
        "'germline_consequence', 'germline_cnv_occurrence'",
    )
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    name = json.loads(args.table_partitions)

    main(to_initialize=name)
