import argparse
import logging
import sys

from radiant.tasks.iceberg import initialization

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(table_name: str):
    if table_name == "database":
        initialization.init_database()
    elif table_name == "germline_snv_occurrence":
        initialization.create_germline_snv_occurrence_table()
    elif table_name == "germline_variant":
        initialization.create_germline_variant_table()
    elif table_name == "germline_consequence":
        initialization.create_germline_consequences_table()
    elif table_name == "germline_cnv_occurrence":
        initialization.create_germline_cnv_occurrence_table()
    else:
        raise ValueError(
            f"Unknown initialization name: {table_name}, possible values are: "
            "'database', 'germline_snv_occurrence', 'germline_variant', "
            "'germline_consequence', 'germline_cnv_occurrence'"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize Iceberg")
    parser.add_argument(
        "--table_name",
        required=True,
        help="Iceberg target to initialize, possible values are: "
        "'database', 'germline_snv_occurrence', 'germline_variant', "
        "'germline_consequence', 'germline_cnv_occurrence'",
    )
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    main(table_name=args.table_name)
