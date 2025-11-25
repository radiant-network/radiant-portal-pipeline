import json
import logging
import sys
import tempfile

from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.iceberg.partition_commit import PartitionCommit
from radiant.tasks.iceberg.table_accumulator import TableAccumulator
from radiant.tasks.iceberg.utils import commit_files
from radiant.tasks.tracing.trace import get_tracer
from radiant.tasks.utils import capture_libc_stderr_and_check_errors, download_s3_file
from radiant.tasks.vcf.experiment import RadiantGermlineAnnotationTask
from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.snv.germline.common import process_common
from radiant.tasks.vcf.snv.germline.consequence import parse_csq_header, process_consequence
from radiant.tasks.vcf.snv.germline.occurrence import process_occurrence
from radiant.tasks.vcf.snv.germline.variant import process_variant

logger = logging.getLogger("airflow.task")
tracer = get_tracer(__name__)

SUPPORTED_CHROMOSOMES = tuple(f"chr{i}" for i in range(1, 23)) + ("chrX", "chrY", "chrM")


# Required decoration because cyvcf2 doesn't fail when it encounters an error, it just prints to stderr.
# Airflow will treat the task as successful if the error is not captured properly.
@capture_libc_stderr_and_check_errors(error_patterns=["[E::"])
def process_task(
    task: RadiantGermlineAnnotationTask,
    catalog_name="default",
    namespace="radiant",
    vcf_threads=None,
    catalog_properties=None,
):
    with tracer.start_as_current_span(f"process_task_{str(task.task_id)}"):
        occurrences_partition_commit = []
        variants_partition_commit = []
        consequences_partition_commit = []
        catalog = (
            load_catalog(catalog_name, **catalog_properties) if catalog_properties else load_catalog(catalog_name)
        )

        vcf = VCF(
            task.vcf_filepath,
            strict_gt=True,
            threads=vcf_threads,
            samples=[exp.aliquot for exp in task.experiments],
        )
        if task.index_vcf_filepath:
            vcf.set_index(index_path=task.index_vcf_filepath)

        occurrences_table_name = f"{namespace}.germline_snv_occurrence"
        variants_table_name = f"{namespace}.germline_snv_variant"
        consequences_table_name = f"{namespace}.germline_snv_consequence"
        if not vcf.samples:
            raise ValueError(f"Task {task.task_id} has no matching samples in the VCF file {task.vcf_filepath}")

        csq_header = parse_csq_header(vcf)
        pedigree = Pedigree(task, vcf.samples)
        with tracer.start_as_current_span(f"vcf_task_{task.task_id}"):
            logger.info(f"Starting processing vcf for task {task.task_id} with file {task.vcf_filepath}")

            occurrence_table = catalog.load_table(occurrences_table_name)
            occurrence_partition_filter = {"part": task.part, "task_id": task.task_id}
            occurrence_buffer = TableAccumulator(occurrence_table, partition_filter=occurrence_partition_filter)

            variant_csq_partition_filter = {"task_id": task.task_id}
            variant_table = catalog.load_table(variants_table_name)
            variant_buffer = TableAccumulator(variant_table, partition_filter=variant_csq_partition_filter)

            consequence_table = catalog.load_table(consequences_table_name)
            consequence_buffer = TableAccumulator(consequence_table, partition_filter=variant_csq_partition_filter)
            for record in vcf:
                if len(record.ALT) <= 1:
                    if record.CHROM in SUPPORTED_CHROMOSOMES:
                        common = process_common(record, task_id=task.task_id, part=task.part)
                        picked_consequence, consequences = process_consequence(record, csq_header, common)
                        consequence_buffer.extend(consequences)
                        occurrences = process_occurrence(record, pedigree, common=common)
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
                PartitionCommit(
                    parquet_files=variant_buffer.parquet_paths, partition_filter=variant_buffer.partition_filter
                )
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


def create_parquet_files(task: dict, namespace: str) -> dict[str, list[dict]]:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    logger.info("Downloading VCF and index files to a temporary directory")
    with tempfile.TemporaryDirectory() as tmpdir:
        vcf_local = download_s3_file(task["vcf_filepath"], tmpdir)
        index_local = download_s3_file(task["vcf_filepath"] + ".tbi", tmpdir)
        task["vcf_filepath"] = vcf_local
        task["index_vcf_filepath"] = index_local

        task = RadiantGermlineAnnotationTask.model_validate(task)
        logger.info(f"🔁 STARTING IMPORT for Task: {task.task_id}")
        logger.info("=" * 80)

        res = process_task(task, namespace=namespace, vcf_threads=4)
        logger.info(f"✅ Parquet files created: {task.task_id}, file {task.vcf_filepath}")

    return {k: [json.loads(pc.model_dump_json()) for pc in v] for k, v in res.items()}


def commit_partitions(table_partitions: dict[str, list[dict]]):
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    catalog = load_catalog()
    for table_name, partitions in table_partitions.items():
        if not partitions:
            continue
        table = catalog.load_table(table_name)
        parts = [PartitionCommit.model_validate(pc) for pc in partitions]
        logger.info(f"🔁 Starting commit for table {table_name}")
        commit_files(table, parts)
        logger.info(f"✅ Changes commited to table {table_name}")
