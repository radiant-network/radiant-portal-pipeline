import argparse
import logging
import sys

from radiant.tasks.iceberg import initialization

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

INITIALIZERS = {
    "database": initialization.init_database,
    "germline_snv_occurrence": initialization.create_germline_snv_occurrence_table,
    "germline_snv_comphet": initialization.create_germline_snv_comphet_table,
    "snv_variant": initialization.create_variant_table,
    "snv_consequence": initialization.create_consequences_table,
    "germline_cnv_occurrence": initialization.create_germline_cnv_occurrence_table,
    "somatic_snv_occurrence": initialization.create_somatic_snv_occurrence_table,
    "somatic_cnv_occurrence": initialization.create_somatic_cnv_occurrence_table,
}


def main(table_name: str):
    INITIALIZERS[table_name]()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize Iceberg")
    parser.add_argument(
        "--table_name",
        required=True,
        choices=INITIALIZERS,
        help="Iceberg target to initialize",
    )
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    try:
        main(table_name=args.table_name)
    except Exception as e:
        logger.exception(f"Error while initializing Iceberg table: {e}")
        sys.exit(1)
