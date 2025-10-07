import itertools
import logging
import time
import uuid
from collections.abc import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Table
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import (
    _determine_partitions,
    _get_parquet_writer_kwargs,
    _to_requested_schema,
    bin_pack_arrow_table,
    compute_statistics_plan,
    data_file_statistics_from_parquet_metadata,
    parquet_path_to_id_mapping,
    pyarrow_to_schema,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema, sanitize_column_names
from pyiceberg.table import WriteTask, load_location_provider
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.typedef import Record
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.config import Config
from pyiceberg.utils.properties import property_as_int

from radiant.tasks.iceberg.partition_commit import PartitionCommit

logger = logging.getLogger("airflow.task")


def merge_schemas(schema1: Schema, schema2: Schema):
    """
    Merge two Iceberg schemas into a single schema.
    """
    appended_fields = list(schema1.fields) + list(schema2.fields)
    return Schema(*appended_fields)


def commit_files(table: Table, partition_to_commit: list[PartitionCommit]):
    """
    Commit all written Parquet files to the Iceberg table using overwrite mode.

    Raises:
        ValueError: If the partition filter is empty.
        CommitFailedException: If commit retries are exhausted.
    """
    if not partition_to_commit:
        logger.info(f"No partitions to commit for table {table.name()} partition filter")
        return

    # Create filter expression for multi-column partition
    table.refresh()
    tx = table.transaction()
    for partition in partition_to_commit:
        filter_expr = None
        for col, val in partition.partition_filter.items():
            expr = EqualTo(col, val)
            filter_expr = expr if filter_expr is None else And(filter_expr, expr)
        if filter_expr is None:
            raise ValueError(
                f"Partition filter {partition.partition_filter} must contain at least one key-value pair."
            )
        tx.delete(filter_expr)
        if partition.parquet_files:
            tx.add_files(partition.parquet_files)

    max_retries = 20
    while max_retries > 0:
        try:
            tx.commit_transaction()
            logger.info("Successfully overwrite partitions")
            break
        except CommitFailedException as e:
            max_retries -= 1
            logger.info(f"Commit failed: {e}. Retries left: {max_retries}")
            time.sleep(1)
            if max_retries <= 0:
                logger.error("Failed after 10 retries. Giving up.")
                raise e


def dataframe_to_data_files(
    table_metadata: TableMetadata,
    df: pa.Table,
    io: FileIO,
    write_uuid: uuid.UUID | None = None,
    counter: Iterator[int] | None = None,
) -> Iterable[DataFile]:
    """Convert a PyArrow table into a DataFile.
    This function is a copy of pyiceberg.io.pyarrow._dataframe_to_data_files.
    These functions are copied to generate parquet files that can be read by any other engine.
    Unfortunately, pyiceberg does not expose these functions as public API.
    We also modified these functions to not include field ids in targeted schema. Otherwise,
    add_files during commit phase fails.

    Returns:
        An iterable that supplies datafiles that represent the table.
    """
    from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE, TableProperties, WriteTask

    counter = counter or itertools.count(0)
    write_uuid = write_uuid or uuid.uuid4()
    target_file_size: int = property_as_int(  # type: ignore  # The property is set with non-None value.
        properties=table_metadata.properties,
        property_name=TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        default=TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
    )
    name_mapping = table_metadata.schema().name_mapping
    downcast_ns_timestamp_to_us = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
    task_schema = pyarrow_to_schema(
        df.schema,
        name_mapping=name_mapping,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
        format_version=table_metadata.format_version,
    )

    if table_metadata.spec().is_unpartitioned():
        yield from _write_file(
            io=io,
            table_metadata=table_metadata,
            tasks=iter(
                [
                    WriteTask(write_uuid=write_uuid, task_id=next(counter), record_batches=batches, schema=task_schema)
                    for batches in bin_pack_arrow_table(df, target_file_size)
                ]
            ),
        )
    else:
        partitions = _determine_partitions(spec=table_metadata.spec(), schema=table_metadata.schema(), arrow_table=df)
        yield from _write_file(
            io=io,
            table_metadata=table_metadata,
            tasks=iter(
                [
                    WriteTask(
                        write_uuid=write_uuid,
                        task_id=next(counter),
                        record_batches=batches,
                        partition_key=partition.partition_key,
                        schema=task_schema,
                    )
                    for partition in partitions
                    for batches in bin_pack_arrow_table(partition.arrow_table_partition, target_file_size)
                ]
            ),
        )


def _write_file(io: FileIO, table_metadata: TableMetadata, tasks: Iterator[WriteTask]) -> Iterator[DataFile]:
    """
    Write Parquet files for the given tasks and return an iterator of DataFile objects.
    This function is a copy of pyiceberg.io.pyarrow.write_file. The modified version does not include field ids in
    targeted schema. Otherwise, function add_files during commit phase fails.
    :param io:
    :param table_metadata:
    :param tasks:
    :return:
    """
    from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE, TableProperties

    parquet_writer_kwargs = _get_parquet_writer_kwargs(table_metadata.properties)
    row_group_size = property_as_int(
        properties=table_metadata.properties,
        property_name=TableProperties.PARQUET_ROW_GROUP_LIMIT,
        default=TableProperties.PARQUET_ROW_GROUP_LIMIT_DEFAULT,
    )
    location_provider = load_location_provider(
        table_location=table_metadata.location, table_properties=table_metadata.properties
    )

    def write_parquet(task: WriteTask) -> DataFile:
        table_schema = table_metadata.schema()
        # if schema needs to be transformed, use the transformed schema and adjust the arrow table accordingly
        # otherwise use the original schema
        if (sanitized_schema := sanitize_column_names(table_schema)) != table_schema:
            file_schema = sanitized_schema
        else:
            file_schema = table_schema

        downcast_ns_timestamp_to_us = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
        batches = [
            _to_requested_schema(
                requested_schema=file_schema,
                file_schema=task.schema,
                batch=batch,
                downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
                include_field_ids=False,  # !!!Do not include field ids in targeted schema,
                # this is needed to avoid issues with add_files during commit phase.
            )
            for batch in task.record_batches
        ]
        arrow_table = pa.Table.from_batches(batches)
        file_path = location_provider.new_data_location(
            data_file_name=task.generate_data_file_filename("parquet"),
            partition_key=task.partition_key,
        )
        fo = io.new_output(file_path)
        with (
            fo.create(overwrite=True) as fos,
            pq.ParquetWriter(
                fos, schema=arrow_table.schema, store_decimal_as_integer=True, **parquet_writer_kwargs
            ) as writer,
        ):
            writer.write(arrow_table, row_group_size=row_group_size)
        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=writer.writer.metadata,
            stats_columns=compute_statistics_plan(file_schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(file_schema),
        )
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=task.partition_key.partition if task.partition_key else Record(),
            file_size_in_bytes=len(fo),
            # After this has been fixed:
            # https://github.com/apache/iceberg-python/issues/271
            # sort_order_id=task.sort_order_id,
            sort_order_id=None,
            # Just copy these from the table for now
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        return data_file

    executor = ExecutorFactory.get_or_create()
    data_files = executor.map(write_parquet, tasks)

    return iter(data_files)
