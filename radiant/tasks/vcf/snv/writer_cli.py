"""Command-line entry points of the ECS SNV writers (`scripts/ecs/import_*_snv_vcf_for_task.py`)."""

import argparse
import json
import logging
import os
import sys
from abc import ABC, abstractmethod

from radiant.tasks.utils import upload_json_to_s3

logger = logging.getLogger(__name__)


class SnvWriter(ABC):
    """Extracts one task to parquet files and stores the result where `merge_commits` reads it."""

    description: str

    @staticmethod
    @abstractmethod
    def create_parquet_files(task: dict, namespace: str) -> dict:
        pass

    def run(self, argv: list[str] | None = None) -> None:
        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])

        parser = argparse.ArgumentParser(description=self.description)
        parser.add_argument("--task", required=True, help="Task JSON string")
        parser.add_argument("--result_uri", required=True, help="S3 URI where the result JSON is written")
        args = parser.parse_args(argv)
        logger.info(f"Command line arguments: {args}")

        try:
            result = self.create_parquet_files(json.loads(args.task), os.environ["RADIANT_ICEBERG_NAMESPACE"])
            upload_json_to_s3(result, args.result_uri)
            logger.info(f"Result written to {args.result_uri}")
        except Exception as e:
            # Exiting non-zero is what makes the ECS task fail: without it a failed extraction is
            # marked successful and its partitions are silently missing from the commit.
            logger.exception(f"Error while processing task: {e}")
            sys.exit(1)


# The process modules are imported in the method, so each script loads only its own flow.
class GermlineSnvWriter(SnvWriter):
    description = "Import Germline SNV VCF for task"

    @staticmethod
    def create_parquet_files(task: dict, namespace: str) -> dict:
        from radiant.tasks.vcf.snv.germline.process import create_parquet_files

        return create_parquet_files(task, namespace)


class SomaticSnvWriter(SnvWriter):
    description = "Import Somatic SNV VCF for task"

    @staticmethod
    def create_parquet_files(task: dict, namespace: str) -> dict:
        from radiant.tasks.vcf.snv.somatic.process import create_parquet_files

        return create_parquet_files(task, namespace)
