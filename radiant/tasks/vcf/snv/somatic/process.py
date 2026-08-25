import json
import logging
import sys
import tempfile
from collections import Counter, namedtuple

from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.iceberg.partition_commit import PartitionCommit
from radiant.tasks.iceberg.table_accumulator import TableAccumulator
from radiant.tasks.utils import capture_libc_stderr_and_check_errors, download_s3_file
from radiant.tasks.vcf.experiment import Experiment, RadiantSomaticAnnotationTask
from radiant.tasks.vcf.snv.common import process_common
from radiant.tasks.vcf.snv.consequence import log_source_counts, parse_csq_header, process_consequence
from radiant.tasks.vcf.snv.somatic.occurrence import process_occurrence
from radiant.tasks.vcf.snv.variant import process_variant

logger = logging.getLogger("airflow.task")


SUPPORTED_CHROMOSOMES = tuple(f"chr{i}" for i in range(1, 23)) + ("chrX", "chrY", "chrM")

SUPPORTED_HISTOLOGY_TYPES = ("tumoral", "normal")

FilteredExperiment = namedtuple("FilteredExperiment", ["tumor_index", "normal_index", "experiments"])


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

    vcf = VCF(task.vcf_filepath, strict_gt=True, threads=vcf_threads)
    # The full sample list has to be captured before narrowing: `set_samples` rewrites both
    # `vcf.samples` and `vcf.raw_header` to the subset, so this is the only chance to see that a
    # one-aliquot (tumor-only) task is really a tumor-normal VCF whose normal experiment is
    # missing or whose aliquot does not match the VCF sample name.
    vcf_file_samples = list(vcf.samples)
    vcf.set_samples([exp.aliquot for exp in task.experiments])
    if task.index_vcf_filepath:
        vcf.set_index(index_path=task.index_vcf_filepath)

    occurrences_table_name = f"{namespace}.somatic_snv_occurrence"
    variants_table_name = f"{namespace}.snv_variant"
    consequences_table_name = f"{namespace}.snv_consequence"
    if not vcf.samples:
        raise ValueError(f"Task {task.task_id} has no matching samples in the VCF file {task.vcf_filepath}")

    # `set_samples` narrows to the intersection and only warns about the rest, so an aliquot the
    # VCF does not carry would otherwise be dropped silently — turning a tumor-normal task handed
    # a tumor-only VCF into a tumor-only analysis.
    missing_aliquots = [exp.aliquot for exp in task.experiments if exp.aliquot not in vcf.samples]
    if missing_aliquots:
        raise ValueError(
            f"Task {task.task_id} declares aliquots {missing_aliquots} that are absent from "
            f"{task.vcf_filepath}, whose samples are {vcf_file_samples} — the task and the VCF "
            f"disagree on the analysis."
        )

    # Here we need to sort the experiments in the same order as the samples appears
    # in the VCF file. This is required because each row contains both the tumor and normal information.
    # Therefore, we need to make sure the index match the order in which the samples appear in the file.
    filtered_experiments = get_sorted_task_experiments(task.experiments, vcf.samples)

    # The aliquot rule alone cannot tell a genuine tumor-only task from a tumor-normal one whose
    # normal experiment never registered upstream. Left silent, that phantom task would be counted
    # in the tumor-only frequency denominator.
    if filtered_experiments.normal_index is None and len(vcf_file_samples) != 1:
        raise ValueError(
            f"Task {task.task_id} resolved to a single tumoral aliquot (tumor-only) but its VCF "
            f"{task.vcf_filepath} declares {len(vcf_file_samples)} samples: {vcf_file_samples} — "
            f"likely a tumor-normal task with a missing or mismatched normal experiment."
        )

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
    source_counts = Counter()

    for record in vcf:
        if len(record.ALT) <= 1:
            if record.CHROM in SUPPORTED_CHROMOSOMES:
                common = process_common(record, task_id=task.task_id, part=task.part)
                picked_consequence, consequences = process_consequence(record, csq_header, common)
                source_counts.update(c["source"] for c in consequences)
                consequence_buffer.extend(consequences)
                occurrences = process_occurrence(
                    record,
                    experiments=filtered_experiments.experiments,
                    common=common,
                    tumor_index=filtered_experiments.tumor_index,
                    normal_index=filtered_experiments.normal_index,
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
    log_source_counts(source_counts, task.task_id, task.vcf_filepath)

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


def get_somatic_indexes(experiments: list[Experiment], samples: list[str]) -> tuple[int, int | None]:
    # A somatic task is tumor-only when it carries exactly one aliquot and that aliquot is tumoral.
    # This is the only path returning `normal_index = None`; requiring the histology keeps a
    # malformed normal-only task from being read as tumor-only.
    if len(experiments) == 1:
        exp = experiments[0]
        if exp.histology_type != "tumoral":
            raise ValueError(
                f"Single-aliquot somatic task has histology_type '{exp.histology_type}', expected 'tumoral': {exp}"
            )
        return samples.index(exp.aliquot), None

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


def get_sorted_task_experiments(experiments: list[Experiment], samples: list[str]) -> FilteredExperiment:
    filtered_experiments = [exp for exp in experiments if exp.aliquot in samples]

    # Validated before resolving indexes: an unsupported histology must surface its own error
    # rather than being reinterpreted by the tumor-only branch of `get_somatic_indexes`.
    if not all([exp.histology_type in SUPPORTED_HISTOLOGY_TYPES for exp in filtered_experiments]):
        raise ValueError("Not all experiments have a valid histology type in task.")

    if len(filtered_experiments) > 2:
        raise ValueError(
            f"Somatic task has {len(filtered_experiments)} aliquots "
            f"({[exp.aliquot for exp in filtered_experiments]}); "
            f"somatic tasks support 1 (tumor-only) or 2 (tumor-normal)."
        )

    tumor_index, normal_index = get_somatic_indexes(filtered_experiments, samples)

    sort_key = {"tumoral": tumor_index}
    if normal_index is not None:
        sort_key["normal"] = normal_index

    sorted_task_experiments = sorted(filtered_experiments, key=lambda x: sort_key[x.histology_type])
    return FilteredExperiment(tumor_index=tumor_index, normal_index=normal_index, experiments=sorted_task_experiments)


def create_parquet_files(task: dict, namespace: str) -> dict[str, list[dict]]:
    """Extract one somatic VCF into parquet files, returning the partitions left to commit.

    Mirrors the germline entrypoint: one call per annotation task, so the DAG can map this
    over every somatic task in the part instead of walking them in a single container.

    A failed download propagates. Skipping it used to be the lesser evil, because one bad VCF
    would otherwise abort the single container that processed the whole part -- but a skip is
    invisible downstream: the task still succeeds, so `update_sequencing_experiments` marks the
    experiment ingested and the incremental delta never offers it again. Now that each task has
    its own mapped instance, failing costs only that instance and the part gets retried.
    """
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)

    logger.info("Downloading VCF and index files to a temporary directory")
    with tempfile.TemporaryDirectory() as tmpdir:
        vcf_local = download_s3_file(task["vcf_filepath"], tmpdir)
        index_local = download_s3_file(task["vcf_filepath"] + ".tbi", tmpdir)

        task_data = {**task, "vcf_filepath": vcf_local, "index_vcf_filepath": index_local}
        radiant_task = RadiantSomaticAnnotationTask.model_validate(task_data)
        logger.info(f"🔁 STARTING IMPORT Somatic SNV for Task: {radiant_task.task_id}")
        logger.info("=" * 80)

        res = process_task(radiant_task, namespace=namespace, vcf_threads=4)
        logger.info(f"✅ Parquet files created: {radiant_task.task_id}, file {radiant_task.vcf_filepath}")

    return {k: [json.loads(pc.model_dump_json()) for pc in v] for k, v in res.items()}
