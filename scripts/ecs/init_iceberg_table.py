import argparse
import logging
import sys

from radiant.tasks.iceberg import initialization

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(table_name: str, mode: str = "create"):
    if mode == "evolve":
        initialization.evolve_table(table_name)
    elif table_name == "database":
        initialization.init_database()
    elif table_name == "germline_snv_occurrence":
        initialization.create_germline_snv_occurrence_table()
    elif table_name == "snv_variant":
        initialization.create_variant_table()
    elif table_name == "snv_consequence":
        initialization.create_consequences_table()
    elif table_name == "germline_cnv_occurrence":
        initialization.create_germline_cnv_occurrence_table()
    elif table_name == "somatic_snv_occurrence":
        initialization.create_somatic_snv_occurrence_table()
    else:
        raise ValueError(
            f"Unknown initialization name: {table_name}, possible values are: "
            "'database', 'germline_snv_occurrence', 'snv_variant', "
            "'snv_consequence', 'germline_cnv_occurrence', 'somatic_snv_occurrence'"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize Iceberg")
    parser.add_argument(
        "--table_name",
        required=True,
        help="Iceberg target to initialize, possible values are: "
        "'database', 'germline_snv_occurrence', 'snv_variant', "
        "'snv_consequence', 'germline_cnv_occurrence', 'somatic_snv_occurrence'",
    )
    parser.add_argument(
        "--mode",
        choices=("create", "evolve"),
        default="create",
        help="'create' drops and recreates the table (destructive, the default). "
        "'evolve' adds any column missing from the existing table and keeps its data.",
    )
    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    try:
        main(table_name=args.table_name, mode=args.mode)
    except Exception as e:
        logger.exception(f"Error while initializing Iceberg table: {e}")
        sys.exit(1)
