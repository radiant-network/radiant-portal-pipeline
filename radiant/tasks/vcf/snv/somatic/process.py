import logging
import sys
import tempfile
from collections import defaultdict

from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.iceberg.partition_commit import PartitionCommit
from radiant.tasks.iceberg.table_accumulator import TableAccumulator
from radiant.tasks.iceberg.utils import commit_files
from radiant.tasks.utils import capture_libc_stderr_and_check_errors, download_s3_file
from radiant.tasks.vcf.experiment import RADIANT_SOMATIC_ANNOTATION_TASK, Experiment, RadiantSomaticAnnotationTask
from radiant.tasks.vcf.snv.common import process_common
from radiant.tasks.vcf.snv.consequence import parse_csq_header, process_consequence
from radiant.tasks.vcf.snv.somatic.occurrence import process_occurrence
from radiant.tasks.vcf.snv.variant import process_variant

logger = logging.getLogger("airflow.task")


SUPPORTED_CHROMOSOMES = tuple(f"chr{i}" for i in range(1, 23)) + ("chrX", "chrY", "chrM")


# Required decoration because cyvcf2 doesn't fail when it encounters an error, it just prints to stderr.
# Airflow will treat the task as successful if the error is not captured properly.
@capture_libc_stderr_and_check_errors(error_patterns=["[E::"])
def process_task(
    task: RadiantSomaticAnnotationTask,
    catalog_name="default",
    namespace="radiant",
    vcf_threads=None,
    catalog_properties=None,
):
    occurrences_partition_commit = []
    variants_partition_commit = []
    consequences_partition_commit = []
    catalog = load_catalog(catalog_name, **(catalog_properties or {}))

    vcf = VCF(
        task.vcf_filepath,
        strict_gt=True,
        threads=vcf_threads,
        samples=[exp.aliquot for exp in task.experiments],
    )
    if task.index_vcf_filepath:
        vcf.set_index(index_path=task.index_vcf_filepath)

    occurrences_table_name = f"{namespace}.somatic_snv_occurrence"
    variants_table_name = f"{namespace}.snv_variant"
    consequences_table_name = f"{namespace}.snv_consequence"
    if not vcf.samples:
        raise ValueError(f"Task {task.task_id} has no matching samples in the VCF file {task.vcf_filepath}")

    csq_header = parse_csq_header(vcf)

    logger.info(f"Starting processing vcf for task {task.task_id} with file {task.vcf_filepath}")
    occurrence_table = catalog.load_table(occurrences_table_name)
    occurrence_partition_filter = {"part": task.part, "task_id": task.task_id}
    occurrence_buffer = TableAccumulator(occurrence_table, partition_filter=occurrence_partition_filter)

    variant_csq_partition_filter = {"task_id": task.task_id}
    variant_table = catalog.load_table(variants_table_name)
    variant_buffer = TableAccumulator(variant_table, partition_filter=variant_csq_partition_filter)

    consequence_table = catalog.load_table(consequences_table_name)
    consequence_buffer = TableAccumulator(consequence_table, partition_filter=variant_csq_partition_filter)

    tumor_index, normal_index = get_somatic_indexes(task.experiments, vcf.samples)

    sort_key = {"tumoral": tumor_index, "normal": normal_index}
    task.experiments = sorted(task.experiments, key=lambda x: sort_key[x.histology_type])

    for record in vcf:
        if len(record.ALT) <= 1:
            if record.CHROM in SUPPORTED_CHROMOSOMES:
                common = process_common(record, task_id=task.task_id, part=task.part)
                picked_consequence, consequences = process_consequence(record, csq_header, common)
                consequence_buffer.extend(consequences)
                occurrences = process_occurrence(
                    record,
                    experiments=task.experiments,
                    common=common,
                    tumor_index=tumor_index,
                    normal_index=normal_index,
                )
                occurrence_buffer.extend(list(occurrences.values()))
                variant = process_variant(record, picked_consequence, common)
                variant_buffer.append(variant)
            else:
                logger.debug(
                    f"Skipped record {record.CHROM} - {record.POS} - {record.ALT} in file {task.vcf_filepath}:"
                    f" this is non supported chromosome."
                )

        else:
            logger.debug(
                f"Skipped record {record.CHROM} - {record.POS} - {record.ALT} in file {task.vcf_filepath}:"
                f" this is a multi allelic variant, mult-allelic are not supported. Please split vcf file."
            )

    #### End of VCF file processing, flush buffers ####
    occurrence_buffer.write_files()
    occurrences_partition_commit.append(
        PartitionCommit(
            parquet_files=occurrence_buffer.parquet_paths,
            partition_filter=occurrence_buffer.partition_filter,
        )
    )

    variant_buffer.write_files()
    variants_partition_commit.append(
        PartitionCommit(parquet_files=variant_buffer.parquet_paths, partition_filter=variant_buffer.partition_filter)
    )

    consequence_buffer.write_files()
    consequences_partition_commit.append(
        PartitionCommit(
            parquet_files=consequence_buffer.parquet_paths,
            partition_filter=consequence_buffer.partition_filter,
        )
    )

    logger.info(f"✅ Parquet files created: {task.task_id}, file {task.vcf_filepath}")
    vcf.close()
    return {
        occurrences_table_name: occurrences_partition_commit,
        variants_table_name: variants_partition_commit,
        consequences_table_name: consequences_partition_commit,
    }


def get_somatic_indexes(experiments: list[Experiment], samples: list[str]):
    tumor_index = None
    normal_index = None
    for exp in experiments:
        if exp.histology_type == "tumoral":
            tumor_index = samples.index(exp.aliquot)
        elif exp.histology_type == "normal":
            normal_index = samples.index(exp.aliquot)
    if tumor_index is None or normal_index is None:
        raise ValueError(
            f"Could not find both tumor and normal samples [{samples}] "
            f"in the VCF for the given experiments: {experiments}."
        )
    return tumor_index, normal_index


def commit_partitions(table_partitions: dict[str, list[dict]], iceberg_catalog_properties: dict | None = None):
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    catalog = load_catalog(**(iceberg_catalog_properties or {}))
    for table_name, partitions in table_partitions.items():
        if not partitions:
            continue
        table = catalog.load_table(table_name)
        parts = [PartitionCommit.model_validate(pc) for pc in partitions]
        logger.info(f"🔁 Starting commit for table {table_name}")
        commit_files(table, parts)
        logger.info(f"✅ Changes committed to table {table_name}")


def merge_partitions_in_place(merged_partitions: defaultdict, partitions_list: dict[str, list[dict]]):
    for table_name, partition_commits in partitions_list.items():
        merged_partitions[table_name].extend(partition_commits)


def import_somatic_snv(tasks: list[dict], namespace: str):
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    merged_partitions = defaultdict(list)

    logger.info("Downloading VCF and index files to a temporary directory")
    for task in tasks:
        if task["task_type"] != RADIANT_SOMATIC_ANNOTATION_TASK:
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_local = download_s3_file(task["vcf_filepath"], tmpdir)
            index_local = download_s3_file(task["vcf_filepath"] + ".tbi", tmpdir)
            task_data = {**task, "vcf_filepath": vcf_local, "index_vcf_filepath": index_local}

            task = RadiantSomaticAnnotationTask.model_validate(task_data)
            logger.info(f"🔁 STARTING IMPORT Somatic SNV for Task: {task_data['task_id']}")
            logger.info("=" * 80)

            partitions = process_task(task, namespace=namespace, vcf_threads=4)
            logger.info(f"✅ Parquet files created: {task_data['task_id']}, file {task_data['vcf_filepath']}")
            merge_partitions_in_place(merged_partitions, partitions)

    # Merge results from all tasks and convert PartitionCommit objects to dicts
    commit_partitions(merged_partitions)
