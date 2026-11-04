import logging
import sys
import tempfile

import pyarrow as pa
from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.tracing.trace import get_tracer
from radiant.tasks.utils import S3DownloadError, download_s3_file
from radiant.tasks.vcf.cnv.germline.occurrence import is_carrier, process_occurrence, process_slivar_occurrence
from radiant.tasks.vcf.experiment import (
    ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK,
    RADIANT_GERMLINE_CNV_ANNOTATION_TASK,
    AlignmentGermlineVariantCallingTask,
    BaseTask,
    RadiantGermlineCnvAnnotationTask,
)
from radiant.tasks.vcf.pedigree import Pedigree

logger = logging.getLogger("airflow.task")
tracer = get_tracer(__name__)

CNV_TASK_TYPES = {
    ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK: AlignmentGermlineVariantCallingTask,
    RADIANT_GERMLINE_CNV_ANNOTATION_TASK: RadiantGermlineCnvAnnotationTask,
}


def _alignment_occurrences(vcf: VCF, task: BaseTask) -> list[dict]:
    occurrences = []
    for exp in task.experiments:
        logger.info(f"Process aliquot: {exp.aliquot} (seq_id: {exp.seq_id})")
        sample_idx = vcf.samples.index(exp.aliquot)
        with tracer.start_as_current_span(f"vcf_task_{task.task_id}_{exp.seq_id}"):
            for record in vcf:
                if not record.ALT:
                    logger.warning(f"Skipping record with no ALT: {record.CHROM}:{record.POS}-{record.end}")
                    continue
                occurrences.append(
                    process_occurrence(
                        record, task.part, exp.seq_id, exp.tenant_code, task.task_id, exp.aliquot, sample_idx
                    )
                )
    return occurrences


def _slivar_occurrences(vcf: VCF, task: RadiantGermlineCnvAnnotationTask) -> list[dict]:
    """The proband's records of the family's slivar VCF, under the proband's sequencing. The parents' and
    the siblings' records give no row: these members keep the DRAGEN set of their alignment task."""
    proband = next((exp for exp in task.experiments if exp.family_role == "proband"), None)
    if proband is None or proband.aliquot not in vcf.samples:
        raise ValueError(f"Task {task.task_id} has no proband sample in the slivar VCF file {task.cnv_vcf_filepath}")

    pedigree = Pedigree(task, vcf.samples)
    sample_idx = vcf.samples.index(proband.aliquot)
    mother_idx = vcf.samples.index(pedigree.mother_experiment.aliquot) if pedigree.mother_experiment else None
    father_idx = vcf.samples.index(pedigree.father_experiment.aliquot) if pedigree.father_experiment else None
    logger.info(
        f"Process proband aliquot: {proband.aliquot} (seq_id: {proband.seq_id}), "
        f"mother in VCF: {mother_idx is not None}, father in VCF: {father_idx is not None}"
    )

    occurrences = []
    with tracer.start_as_current_span(f"vcf_task_{task.task_id}_{proband.seq_id}"):
        for record in vcf:
            if not record.ALT:
                logger.warning(f"Skipping record with no ALT: {record.CHROM}:{record.POS}-{record.end}")
                continue
            # The family VCF holds every member's calls: a `0/0` or `./.` record of the proband is a relative's CNV.
            if not is_carrier(record.genotypes[sample_idx][:-1]):
                continue
            occurrences.append(
                process_slivar_occurrence(
                    record,
                    task.part,
                    proband.seq_id,
                    proband.tenant_code,
                    task.task_id,
                    proband.aliquot,
                    sample_idx,
                    mother_idx,
                    father_idx,
                )
            )
    return occurrences


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

        occurrences_table_name = f"{namespace}.germline_cnv_occurrence"
        if not tasks:
            # Overwriting with an empty buffer would wipe the table; refuse the no-op.
            logger.warning(f"No CNV tasks to process; skipping overwrite of {occurrences_table_name}")
            return {occurrences_table_name: occurrences_partition_commit}

        catalog = (
            load_catalog(catalog_name, **catalog_properties) if catalog_properties else load_catalog(catalog_name)
        )

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
            if task.task_type == RADIANT_GERMLINE_CNV_ANNOTATION_TASK:
                occurrence_buffer.extend(_slivar_occurrences(vcf, task))
            else:
                occurrence_buffer.extend(_alignment_occurrences(vcf, task))
            vcf.close()
        df = pa.Table.from_pylist(occurrence_buffer, schema=occurrence_table.schema().as_arrow())
        occurrence_table.overwrite(df)
        logger.info(f"✅ Table {occurrences_table_name} overwritten")

        return {occurrences_table_name: occurrences_partition_commit}


def import_cnv_vcf(tasks: list[dict], namespace: str) -> None:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    updated_tasks = []
    attempted = 0
    skipped = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        for task in tasks:
            task_cls = CNV_TASK_TYPES.get(task.get("task_type"))
            if task_cls is None:
                continue
            if task.get("deleted"):
                logger.info(f"Skipping deleted CNV task [{task.get('task_id')}]")
                continue

            attempted += 1
            # No index is fetched, unlike SNV: the whole file is iterated, never queried by region.
            logger.info(f"Downloading VCF from {task['cnv_vcf_filepath']} to a temporary directory")
            try:
                cnv_vcf_local = download_s3_file(task["cnv_vcf_filepath"], tmpdir, randomize_filename=True)
            except S3DownloadError as e:
                if task.get("task_type") == RADIANT_GERMLINE_CNV_ANNOTATION_TASK:
                    raise RuntimeError(
                        f"Failed to download the slivar VCF of task [{task.get('task_id')}] "
                        f"from {task['cnv_vcf_filepath']}: {e}"
                    ) from e
                skipped += 1
                logger.warning(
                    f"Failed to download CNV VCF for task [{task.get('task_id')}] "
                    f"from {task['cnv_vcf_filepath']}, skipping: {e}",
                    exc_info=True,
                )
                continue
            logger.info(f"Downloaded CNV VCF to {cnv_vcf_local}")
            task["cnv_vcf_filepath"] = cnv_vcf_local

            task = task_cls.model_validate(task)
            updated_tasks.append(task)

        if skipped:
            logger.warning(f"Skipped {skipped}/{attempted} CNV tasks due to download failures")

        # If all files failed, we abort
        if attempted and not updated_tasks:
            raise RuntimeError(f"All {attempted} Germline CNV download(s) failed")

        logger.info("Starting CNV VCF processing for all tasks")
        process_tasks(updated_tasks, namespace=namespace, vcf_threads=4)
