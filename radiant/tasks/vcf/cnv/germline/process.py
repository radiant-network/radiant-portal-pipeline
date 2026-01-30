import logging
import sys
import tempfile

import pyarrow as pa
from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.tracing.trace import get_tracer
from radiant.tasks.utils import download_s3_file
from radiant.tasks.vcf.cnv.germline.occurrence import process_occurrence
from radiant.tasks.vcf.experiment import (
    ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK,
    AlignmentGermlineVariantCallingTask,
    BaseTask,
)

logger = logging.getLogger("airflow.task")
tracer = get_tracer(__name__)


# Required decoration because cyvcf2 doesn't fail when it encounters an error, it just prints to stderr.
# Airflow will treat the task as successful if the error is not captured properly.
# @capture_libc_stderr_and_check_errors(error_patterns=["[E::"])
def process_tasks(
    tasks: list[BaseTask],
    catalog_name="default",
    namespace="radiant",
    vcf_threads=None,
    catalog_properties=None,
):
    with tracer.start_as_current_span("process_tasks"):
        occurrences_partition_commit = []

        catalog = (
            load_catalog(catalog_name, **catalog_properties) if catalog_properties else load_catalog(catalog_name)
        )

        occurrences_table_name = f"{namespace}.germline_cnv_occurrence"
        occurrence_table = catalog.load_table(occurrences_table_name)
        occurrence_buffer = []
        for task in tasks:
            if not task.cnv_vcf_filepath:
                logger.info(f"No CNV VCF filepath for task [{task.task_id}] skipping")
                continue

            vcf = VCF(
                task.cnv_vcf_filepath,
                strict_gt=True,
                threads=vcf_threads,
                samples=[exp.aliquot for exp in task.experiments],
            )
            if not vcf.samples:
                raise ValueError(
                    f"Task {task.task_id} has no matching samples in the CNV VCF file {task.cnv_vcf_filepath}"
                )

            logger.info(f"Starting processing CNV VCF for task {task.task_id} with file {task.cnv_vcf_filepath}")
            for exp in task.experiments:
                logger.info(f"Process aliquot: {exp.aliquot} (seq_id: {exp.seq_id})")
                sample_idx = vcf.samples.index(exp.aliquot)
                part = 0
                with tracer.start_as_current_span(f"vcf_task_{task.task_id}_{exp.seq_id}"):
                    for record in vcf:
                        if not record.ALT:
                            logger.warning(f"Skipping record with no ALT: {record.CHROM}:{record.POS}-{record.end}")
                            continue

                        logger.info(
                            f"Processing record {record.CHROM}:{record.POS}-{record.end}-{record.ALT} "
                            f"for sample {exp.aliquot}"
                        )
                        occurrence = process_occurrence(record, part, exp.seq_id, exp.aliquot, sample_idx)
                        occurrence_buffer.append(occurrence)
                    vcf.close()
        df = pa.Table.from_pylist(occurrence_buffer, schema=occurrence_table.schema().as_arrow())
        occurrence_table.overwrite(df)
        logger.info(f"✅ Table {occurrences_table_name} overwritten")

        return {occurrences_table_name: occurrences_partition_commit}


def import_cnv_vcf(tasks: list[dict], namespace: str) -> None:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    updated_tasks = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for task in tasks:
            if task.get("task_type") != ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK:
                continue

            logger.info(f"Downloading VCF and index files from {task['cnv_vcf_filepath']} to a temporary directory")
            cnv_vcf_local = download_s3_file(task["cnv_vcf_filepath"], tmpdir, randomize_filename=True)
            logger.info(f"Downloaded CNV VCF to {cnv_vcf_local}")
            task["cnv_vcf_filepath"] = cnv_vcf_local

            task = AlignmentGermlineVariantCallingTask.model_validate(task)
            updated_tasks.append(task)

        logger.info("Starting CNV VCF processing for all tasks")
        process_tasks(updated_tasks, namespace=namespace, vcf_threads=4)
