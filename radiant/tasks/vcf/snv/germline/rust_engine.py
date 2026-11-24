"""Germline SNV extraction through the Rust engine (``rust/radiant_vcf``, POC).

The Rust extension reads the VCF, builds the rows of the four germline SNV tables and writes them to local Parquet
files (arrow-rs ``parquet`` crate), with the writer options PyIceberg takes from the table properties. This module
uploads those files where PyIceberg would have written them and returns the same ``PartitionCommit`` list as the
Python path, so the Iceberg commit (``add_files``) does not change. Select it with ``RADIANT_SNV_ENGINE=rust``; the
Python path stays the default.

Parity with the Python path is checked by ``tests/unit/vcf/germline/test_rust_engine_parity.py``.
"""

import logging
import os
import shutil
import tempfile
import uuid
from collections import Counter

from pyiceberg.io.pyarrow import PyArrowFileIO, _get_parquet_writer_kwargs
from pyiceberg.partitioning import PartitionFieldValue, PartitionKey
from pyiceberg.table import TableProperties, load_location_provider
from pyiceberg.utils.properties import property_as_int

from radiant.tasks.iceberg.partition_commit import PartitionCommit
from radiant.tasks.iceberg.table_accumulator import TableAccumulator, resolve_parquet_file_size_mb
from radiant.tasks.vcf.experiment import RadiantGermlineAnnotationTask
from radiant.tasks.vcf.snv.consequence import log_invalid_cds, log_source_counts

logger = logging.getLogger("airflow.task")

ENGINE_ENV = "RADIANT_SNV_ENGINE"
ENGINE_PYTHON = "python"
ENGINE_RUST = "rust"

# Records decoded between two writes to the Parquet files.
RUST_BATCH_RECORDS = 10_000

UPLOAD_CHUNK_BYTES = 8 * 1024 * 1024


def snv_engine() -> str:
    """The germline SNV engine, from ``RADIANT_SNV_ENGINE`` (default ``python``). Unknown values raise."""
    value = (os.getenv(ENGINE_ENV) or ENGINE_PYTHON).strip().lower()
    if value not in (ENGINE_PYTHON, ENGINE_RUST):
        raise ValueError(f"{ENGINE_ENV} must be '{ENGINE_PYTHON}' or '{ENGINE_RUST}', got {value!r}")
    return value


def sink_config(table, directory: str, prefix: str) -> dict:
    """Writer options of one table, as ``radiant/tasks/iceberg/utils.py::_write_file`` resolves them.

    Two differences, both about when a writer flushes rather than what it writes: a row group is also closed at
    ``RADIANT_PARQUET_FILE_SIZE_MB`` of encoded data (the Python path's memory bound, the accumulator flush), and a
    file is closed at ``write.target-file-size-bytes`` of Parquet bytes (the Python path bins on Arrow bytes).
    """
    props = table.metadata.properties
    writer = _get_parquet_writer_kwargs(props)
    return {
        "schema": TableAccumulator.remove_field_ids(table.schema().as_arrow()),
        "directory": directory,
        "prefix": prefix,
        "compression": writer["compression"],
        "compression_level": writer["compression_level"],
        "data_page_size": writer["data_page_size"],
        "dictionary_page_size": writer["dictionary_pagesize_limit"],
        "write_batch_size": writer["write_batch_size"],
        "row_group_rows": property_as_int(
            props, TableProperties.PARQUET_ROW_GROUP_LIMIT, TableProperties.PARQUET_ROW_GROUP_LIMIT_DEFAULT
        ),
        "row_group_bytes": resolve_parquet_file_size_mb() * 1024 * 1024,
        "target_file_size": property_as_int(
            props, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        ),
    }


def partition_key(table, partition_filter: dict) -> PartitionKey | None:
    """The partition of the files: the tables are identity-partitioned on the columns of the partition filter.

    None for an unpartitioned table, as `_dataframe_to_data_files` does: the files then go straight under `data/`.
    """
    schema, spec = table.schema(), table.spec()
    if spec.is_unpartitioned():
        return None
    values = []
    for field in spec.fields:
        name = schema.find_field(field.source_id).name
        if name not in partition_filter:
            raise ValueError(
                f"{table.name()}: partition column {name} is not in the partition filter {partition_filter}"
            )
        values.append(PartitionFieldValue(field, partition_filter[name]))
    return PartitionKey(field_values=values, partition_spec=spec, schema=schema)


def upload_files(table, partition_filter: dict, local_files: list[tuple[str, int, int]]) -> list[str]:
    """Copy the local Parquet files to the table's data location, named as ``_write_file`` names them."""
    if not local_files:
        return []
    io = PyArrowFileIO(table.catalog.properties)
    location = load_location_provider(
        table_location=table.metadata.location, table_properties=table.metadata.properties
    )
    key = partition_key(table, partition_filter)
    write_uuid = uuid.uuid4()
    paths = []
    for counter, (local_path, _, _) in enumerate(local_files):
        path = location.new_data_location(data_file_name=f"00000-{counter}-{write_uuid}.parquet", partition_key=key)
        with open(local_path, "rb") as src, io.new_output(path).create(overwrite=True) as dst:
            shutil.copyfileobj(src, dst, UPLOAD_CHUNK_BYTES)
        paths.append(path.replace("s3a", "s3"))
    logger.info(f"Uploaded {len(paths)} files to {table.name()}: {paths}")
    return paths


def process_task_rust(task: RadiantGermlineAnnotationTask, catalog, namespace: str, vcf_threads=None) -> dict:
    """``process_task`` through the Rust engine. Same tables, same partition filters, same return value."""
    # Imported here: the extension is only required when the Rust engine is selected.
    import radiant_vcf

    experiments = [
        (e.aliquot, e.seq_id, e.tenant_code, e.family_role, e.affected_status, e.sex) for e in task.experiments
    ]
    try:
        reader = radiant_vcf.GermlineSnvReader(
            task.vcf_filepath, experiments, task.task_id, task.part, threads=vcf_threads
        )
    except RuntimeError as e:
        # Same error as the Python path for a VCF without any of the task's aliquots.
        if "has no matching samples" in str(e):
            raise ValueError(str(e)) from e
        raise

    logger.info(
        f"Task {task.task_id}: parental origin from {'slivar' if reader.slivar_parental_origin else 'etl'}, "
        f"transmission mode from {'slivar' if reader.slivar_transmission_mode else 'etl'} (rust engine)"
    )

    table_names = {
        "occurrence": f"{namespace}.germline_snv_occurrence",
        "variant": f"{namespace}.snv_variant",
        "consequence": f"{namespace}.snv_consequence",
        "comphet": f"{namespace}.germline_snv_comphet",
    }
    occurrence_filter = {"part": task.part, "task_id": task.task_id}
    variant_csq_filter = {"task_id": task.task_id}
    filters = {
        "occurrence": occurrence_filter,
        "variant": variant_csq_filter,
        "consequence": variant_csq_filter,
        # Same partition filter as the occurrences: a re-imported task replaces its pairs, even with none.
        "comphet": occurrence_filter,
    }
    tables = {key: catalog.load_table(name) for key, name in table_names.items()}

    result = {}
    with tempfile.TemporaryDirectory(prefix=f"snv-{task.task_id}-") as tmp:
        sinks = {key: sink_config(table, os.path.join(tmp, key), key) for key, table in tables.items()}
        logger.info(f"Starting processing vcf for task {task.task_id} with file {task.vcf_filepath} (rust engine)")
        files = reader.write_parquet(sinks, RUST_BATCH_RECORDS)
        _log_stats(reader.stats(), task)
        for key, name in table_names.items():
            rows = sum(f[1] for f in files[key])
            logger.info(f"{name}: {rows} rows in {len(files[key])} Parquet files")
            paths = upload_files(tables[key], filters[key], files[key])
            result[name] = [PartitionCommit(parquet_files=paths, partition_filter=filters[key])]
    logger.info(f"✅ Parquet files created: {task.task_id}, file {task.vcf_filepath} (rust engine)")
    return result


def _log_stats(stats: dict, task: RadiantGermlineAnnotationTask) -> None:
    counts = stats["counts"]
    source_counts = Counter(stats["source_counts"])
    source_counts[None] = counts["unclassified_sources"]
    log_source_counts(source_counts, task.task_id, task.vcf_filepath)
    log_invalid_cds(Counter(stats["invalid_cds"]), task.task_id, task.vcf_filepath)
    for value in stats["unknown_sources"]:
        logger.warning(f"Unexpected CSQ SOURCE value {value!r}: fell back to the transcript and gene identifiers.")
    if counts["malformed_comphet"]:
        examples = ", ".join(f"'{entry}' at {locus}" for entry, locus in stats["malformed_comphet_examples"][:5])
        logger.warning(f"Skipped {counts['malformed_comphet']} malformed slivar_comphet entries (e.g. {examples})")
    logger.info(f"Task {task.task_id}: skipped {counts['skipped_no_alt']} records with no alt call in any sample")
    logger.info(
        f"Task {task.task_id}: {counts['records']} records read, {counts['kept']} kept, "
        f"{counts['skipped_multi_allelic']} multi-allelic and {counts['skipped_unsupported_chromosome']} on "
        f"unsupported chromosomes skipped"
    )
