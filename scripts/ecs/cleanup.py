import argparse
import logging
import sys

from radiant.tasks.utils import delete_s3_object

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 path to cleanup")

    parser.add_argument(
        "--path",
        required=True,
        help="S3 path to a file to be deleted",
    )
    args = parser.parse_args()
    logger.info(f"Received argument --path={args.path}")

    try:
        delete_s3_object(args.path, logger)
    except Exception as e:
        logger.exception(f"Error while cleaning up path: {e}")
        sys.exit(1)
