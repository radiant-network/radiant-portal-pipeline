import logging
import sys
import tempfile

import pyarrow as pa
from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.tracing.trace import get_tracer
from radiant.tasks.utils import download_s3_file
from radiant.tasks.vcf.cnv.somatic.occurrence import process_occurrence, resolve_cnv_type
from radiant.tasks.vcf.experiment import (
    TUMOR_ONLY_VARIANT_CALLING_TASK,
    BaseTask,
    TumorOnlyVariantCallingTask,
)

logger = logging.getLogger("airflow.task")
tracer = get_tracer(__name__)


def validate_task_is_tumor_only(task: BaseTask, vcf_file_samples: list[str]) -> None:
    """Holds the file to what the task type claims.

    `tumor_only_variant_calling` asserts a single tumoral sample, but only
    `gather_additional_args` enforces that, and only on the `build_task_from_rows` path -- the K8s and
    ECS containers reach this module through `model_validate` on a dict, which skips it. So the checks
    are repeated here, where the file itself is available.

    The sample count is the only one that catches a genuinely tumor-normal file mislabelled upstream.
    Tumor-normal CNV is out of scope, so all three fail loudly rather than half-succeed by writing a
    normal sample's depths as tumor values.
    """
    if len(task.experiments) != 1:
        raise ValueError(
            f"Task {task.task_id} is `{TUMOR_ONLY_VARIANT_CALLING_TASK}` but carries "
            f"{len(task.experiments)} experiments ({[exp.aliquot for exp in task.experiments]}); "
            f"expected exactly one tumoral sample."
        )

    experiment = task.experiments[0]
    if experiment.histology_type != "tumoral":
        raise ValueError(
            f"Task {task.task_id} is `{TUMOR_ONLY_VARIANT_CALLING_TASK}` but its experiment "
            f"{experiment.aliquot} has histology_type '{experiment.histology_type}', expected 'tumoral'."
        )

    if len(vcf_file_samples) != 1:
        raise ValueError(
            f"Task {task.task_id} is `{TUMOR_ONLY_VARIANT_CALLING_TASK}` but its VCF "
            f"{task.cnv_vcf_filepath} declares {len(vcf_file_samples)} samples: {vcf_file_samples} -- "
            f"likely a tumor-normal file mislabelled upstream."
        )


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

        occurrences_table_name = f"{namespace}.somatic_cnv_occurrence"
        if not tasks:
            # Overwriting with an empty buffer would wipe the table; refuse the no-op.
            logger.warning(f"No somatic CNV tasks to process; skipping overwrite of {occurrences_table_name}")
            return {occurrences_table_name: occurrences_partition_commit}

        catalog = (
            load_catalog(catalog_name, **catalog_properties) if catalog_properties else load_catalog(catalog_name)
        )

        occurrence_table = catalog.load_table(occurrences_table_name)
        occurrence_buffer = []
        for task in tasks:
            if not task.cnv_vcf_filepath:
                logger.info(f"No somatic CNV VCF filepath for task [{task.task_id}] skipping")
                continue

            vcf = VCF(task.cnv_vcf_filepath, strict_gt=True, threads=vcf_threads)
            # The full sample list has to be captured before narrowing: `set_samples` rewrites both
            # `vcf.samples` and `vcf.raw_header` to the subset, so this is the only chance to see that
            # a one-aliquot task was handed a tumor-normal file.
            vcf_file_samples = list(vcf.samples)
            validate_task_is_tumor_only(task, vcf_file_samples)

            vcf.set_samples([exp.aliquot for exp in task.experiments])
            if not vcf.samples:
                raise ValueError(
                    f"Task {task.task_id} has no matching samples in the somatic CNV VCF file "
                    f"{task.cnv_vcf_filepath}; its samples are {vcf_file_samples}."
                )

            logger.info(
                f"Starting processing somatic CNV VCF for task {task.task_id} with file {task.cnv_vcf_filepath}"
            )
            for exp in task.experiments:
                logger.info(f"Process aliquot: {exp.aliquot} (seq_id: {exp.seq_id})")
                sample_idx = vcf.samples.index(exp.aliquot)
                with tracer.start_as_current_span(f"vcf_task_{task.task_id}_{exp.seq_id}"):
                    for record in vcf:
                        if not record.ALT:
                            # Reference segments (`DRAGEN:REF:...`, ALT `.`) carry no event.
                            logger.debug(f"Skipping record with no ALT: {record.CHROM}:{record.POS}-{record.end}")
                            continue

                        # An ALT the mapping does not recognise would produce a NULL `cnv_id` against a
                        # NOT NULL key column and fail the whole StarRocks load, not just its own row.
                        cnv_type = resolve_cnv_type(record)
                        if cnv_type is None:
                            logger.warning(
                                f"Skipping unclassifiable CNV record {record.CHROM}:{record.POS}-{record.end} "
                                f"[{record.ID}] with ALT {record.ALT}"
                            )
                            continue

                        occurrence = process_occurrence(
                            record,
                            task.part,
                            exp.seq_id,
                            exp.tenant_code,
                            task.task_id,
                            exp.aliquot,
                            sample_idx,
                            cnv_type,
                        )
                        occurrence_buffer.append(occurrence)
            vcf.close()
        df = pa.Table.from_pylist(occurrence_buffer, schema=occurrence_table.schema().as_arrow())
        occurrence_table.overwrite(df)
        logger.info(f"✅ Table {occurrences_table_name} overwritten")

        return {occurrences_table_name: occurrences_partition_commit}


def import_somatic_cnv_vcf(tasks: list[dict], namespace: str) -> None:
    """Extracts every somatic tumor-only CNV VCF of a part into the somatic CNV occurrence table.

    A failed download propagates, unlike germline CNV, which logs and skips it (SJRA-1784). A skip is
    invisible downstream: the task still succeeds, so `update_sequencing_experiments` marks the
    experiment ingested and the incremental delta never offers it again. Failing costs a retry of the
    part; skipping costs the data permanently.
    """
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    updated_tasks = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for task in tasks:
            if task.get("task_type") != TUMOR_ONLY_VARIANT_CALLING_TASK:
                continue

            if not task.get("cnv_vcf_filepath"):
                # The staging view's `scnv` gate should always supply the URL; a missing one is a
                # tumor-only task with nothing to extract, not a failure.
                logger.info(f"No somatic CNV VCF filepath for task [{task.get('task_id')}] skipping")
                continue

            logger.info(f"Downloading somatic CNV VCF from {task['cnv_vcf_filepath']} to a temporary directory")
            cnv_vcf_local = download_s3_file(task["cnv_vcf_filepath"], tmpdir, randomize_filename=True)
            logger.info(f"Downloaded somatic CNV VCF to {cnv_vcf_local}")
            task["cnv_vcf_filepath"] = cnv_vcf_local

            task = TumorOnlyVariantCallingTask.model_validate(task)
            updated_tasks.append(task)

        logger.info("Starting somatic CNV VCF processing for all tasks")
        process_tasks(updated_tasks, namespace=namespace, vcf_threads=4)
