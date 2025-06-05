import itertools
import logging
import os
import time
import uuid
from typing import Optional, Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Table
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo
from pyiceberg.io import S3_ENDPOINT
from pyiceberg.io.pyarrow import PyArrowFileIO, pyarrow_to_schema, compute_statistics_plan, parquet_path_to_id_mapping, \
    data_file_statistics_from_parquet_metadata, bin_pack_arrow_table, _get_parquet_writer_kwargs, _determine_partitions, \
    _to_requested_schema
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import sanitize_column_names
from pyiceberg.table import WriteTask, load_location_provider
from pyiceberg.typedef import Record
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.config import Config
from pyiceberg.utils.properties import property_as_int
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.io import FileIO

logger = logging.getLogger("airflow.task")

PARQUET_FILE_SIZE_MB = 1024
MAX_BUFFERED_ROWS = 10000


class TableAccumulator:
    """
    Accumulates rows in memory and writes them to Apache Iceberg table partitions
    as Parquet files, with configurable batching and commit behavior.

    This class is useful for buffering data rows in memory and writing them in batches,
    reducing I/O overhead and ensuring Parquet file size thresholds are met before
    committing to the Iceberg table.

    Attributes:
        table (Table): The target Iceberg table.
        fs (Optional[FileSystem]): Optional filesystem for writing Parquet files.
        partition_filter (dict): Partition spec to use for writing and overwriting data.
        max_buffered_rows (int): Maximum number of rows to buffer before a merge.
        parquet_file_size_mb (int): Size threshold (in MB) for writing Parquet files.
    """

    def __init__(
            self,
            table: Table,
            fs=None,
            partition_filter: dict = None,
            max_buffered_rows: int = MAX_BUFFERED_ROWS,
            parquet_file_size_mb: int = PARQUET_FILE_SIZE_MB,
    ):
        """
        Initialize the TableAccumulator.

        Args:
            table (Table): The target Iceberg table instance.
            fs: Optional PyArrow filesystem object for writing Parquet files.
            partition_filter (dict, optional): Partition key-value pairs to overwrite.
            max_buffered_rows (int): Maximum number of rows to buffer before merging.
            parquet_file_size_mb (int): Parquet file size threshold in MB for writing.
        """
        self.table = table
        self.schema = self.remove_field_ids(table.schema().as_arrow())
        self.partition_filter = partition_filter or {}
        self.fs = fs
        self.parquet_paths = []
        self.rows = []
        self.accumulated_pa_table = None
        self.max_buffered_rows = max_buffered_rows
        self.parquet_file_size_mb = parquet_file_size_mb

    @staticmethod
    def remove_field_ids(schema: pa.Schema) -> pa.Schema:
        """
        Recursively remove Iceberg field IDs from an Arrow schema.

        Args:
            schema (pa.Schema): An Arrow schema that may include Iceberg field IDs.

        Returns:
            pa.Schema: A schema without any field IDs.
        """
        def strip_field_ids(field: pa.Field) -> pa.Field:
            field_type = field.type
            if pa.types.is_struct(field_type):
                # Recursively clean struct fields
                new_fields = [strip_field_ids(f) for f in field_type]
                new_type = pa.struct(new_fields)
            elif pa.types.is_list(field_type):
                # List has a single value field
                value_field = strip_field_ids(field_type.value_field)
                new_type = pa.list_(value_field)
            elif pa.types.is_large_list(field_type):
                value_field = strip_field_ids(field_type.value_field)
                new_type = pa.large_list(value_field)
            elif pa.types.is_map(field_type):
                # Map has key and item fields
                new_type = pa.map_(
                    strip_field_ids(field_type.key_field),
                    strip_field_ids(field_type.item_field),
                    keys_sorted=field_type.keys_sorted
                )
            else:
                new_type = field_type  # Primitive type

            return pa.field(field.name, new_type, nullable=field.nullable)

        new_fields = [strip_field_ids(f) for f in schema]
        return pa.schema(new_fields)

    def append(self, row: dict):
        """
        Append a single row to the in-memory buffer.

        Args:
            row (dict): A row of data to be added.
        """
        self.rows.append(row)
        self.merge_table()

    def extend(self, rows: list[dict]):
        """
        Extend the buffer with multiple rows.

        Args:
            rows (list[dict]): A list of data rows to append.
        """
        self.rows.extend(rows)
        self.merge_table()

    def to_arrow(self) -> pa.Table:
        """
        Convert the current in-memory rows to an Arrow table.

        Returns:
            pa.Table: A PyArrow Table constructed from buffered rows.
        """
        return pa.Table.from_pylist(self.rows, schema=self.schema)

    def clear_rows(self):
        """
        Clear the in-memory row buffer.
        """
        self.rows.clear()

    def merge_table(self, force: bool = False):
        """
        Merge buffered rows into the accumulated table and optionally write to disk
        if file size threshold is reached.

        Args:
            force (bool): Force a merge regardless of row count threshold.
        """
        if force or len(self.rows) >= self.max_buffered_rows:
            logger.debug(f"Compact table {self.table.name()} for partition {self.partition_filter}")
            new_pa_table = self.to_arrow()
            if not self.accumulated_pa_table:
                self.accumulated_pa_table = new_pa_table
            else:
                self.accumulated_pa_table = pa.concat_tables([self.accumulated_pa_table, new_pa_table])
            self.clear_rows()

            if self.accumulated_pa_table.get_total_buffer_size() >= self.parquet_file_size_mb * 1024 * 1024:
                self.write_arrow_to_iceberg()

    def write_files(self, commit: bool = True, merge=True) -> str | None:
        """
        Write the accumulated Arrow table to a Parquet file, and optionally
        commit the file to the Iceberg table.

        Args:
            commit (bool): Whether to commit the written file to Iceberg.
            merge (bool): Whether to force a merge before writing.

        Returns:
            str | None: The Parquet file path if written, otherwise None.
        """
        if merge:
            self.merge_table(force=True)
        if not self.accumulated_pa_table:
            logger.info(f"No data to write for table {self.table.name()}, partition {self.partition_filter}")
            return None

        file_uuid = str(uuid.uuid4())
        parts = [f"{k}={v}" for k, v in self.partition_filter.items()]
        table_location = self.table.location().replace("s3a://", "s3://")
        parquet_path = os.path.join(table_location, *parts, f"{file_uuid}.parquet")
        self.parquet_paths.append(parquet_path)

        logger.info(f"Writing to parquet path: {parquet_path}")

        with self.fs.open(parquet_path, "wb") as fos, pq.ParquetWriter(fos, schema=self.schema) as writer:
            writer.write_table(self.accumulated_pa_table)

        self.accumulated_pa_table = None
        if commit:
            self.commit_files()
        return parquet_path

    def write_arrow_to_iceberg(self):
        logger.info(f"Write arrow to iceberg")
        self.merge_table(force=True)

        if self.accumulated_pa_table:
            file_io = PyArrowFileIO({S3_ENDPOINT: "https://objets.juno.calculquebec.ca"})
            counter = itertools.count()
            uuid_ = uuid.uuid4()
            files = list(
                dataframe_to_data_files(self.table.metadata, self.accumulated_pa_table, file_io, uuid_, counter))
            logger.info(f"Writing {len(files)} files to iceberg")
            logger.info(f"files {files}")
            self.parquet_paths.extend([f.file_path.replace("s3a", "s3") for f in files])

    def commit_files(self):
        """
        Commit all written Parquet files to the Iceberg table using overwrite mode.

        Raises:
            ValueError: If the partition filter is empty.
            CommitFailedException: If commit retries are exhausted.
        """
        if not self.parquet_paths:
            logger.info(
                f"No parquet files to commit for table {self.table.name()} partition filter {self.partition_filter}"
            )
            return
        # Create filter expression for multi-column partition
        filter_expr = None
        for col, val in self.partition_filter.items():
            expr = EqualTo(col, val)
            filter_expr = expr if filter_expr is None else And(filter_expr, expr)
        if filter_expr is None:
            raise ValueError(f"Partition filter {self.partition_filter} must contain at least one key-value pair.")
        max_retries = 200
        while max_retries > 0:
            try:
                logger.info(f"Try to commit tx for table {self.table.name()} with filter {self.partition_filter}")
                self.table.refresh()
                tx = self.table.transaction()
                tx.delete(filter_expr)
                tx.add_files(self.parquet_paths)
                tx.commit_transaction()

                logger.info(f"Successfully overwrite partition {self.partition_filter}")
                break

            except CommitFailedException as e:
                max_retries -= 1
                logger.info(f"Commit failed: {e}. Retries left: {max_retries}")
                time.sleep(1)
                if max_retries <= 0:
                    logger.error("Failed after 200 retries. Giving up.")
                    raise e

def dataframe_to_data_files(
        table_metadata: TableMetadata,
        df: pa.Table,
        io: FileIO,
        write_uuid: Optional[uuid.UUID] = None,
        counter: Optional[Iterator[int]] = None,
) -> Iterable[DataFile]:
    """Convert a PyArrow table into a DataFile.

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
    from pyiceberg.utils.config import Config
    downcast_ns_timestamp_to_us = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
    task_schema = pyarrow_to_schema(df.schema, name_mapping=name_mapping, downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us)

    if table_metadata.spec().is_unpartitioned():
        yield from write_file(
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
        yield from write_file(
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


def write_file(io: FileIO, table_metadata: TableMetadata, tasks: Iterator[WriteTask]) -> Iterator[DataFile]:
    from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE, TableProperties

    parquet_writer_kwargs = _get_parquet_writer_kwargs(table_metadata.properties)
    row_group_size = property_as_int(
        properties=table_metadata.properties,
        property_name=TableProperties.PARQUET_ROW_GROUP_LIMIT,
        default=TableProperties.PARQUET_ROW_GROUP_LIMIT_DEFAULT,
    )
    location_provider = load_location_provider(table_location=table_metadata.location, table_properties=table_metadata.properties)

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
                include_field_ids=False,
            )
            for batch in task.record_batches
        ]
        arrow_table = pa.Table.from_batches(batches)
        file_path = location_provider.new_data_location(
            data_file_name=task.generate_data_file_filename("parquet"),
            partition_key=task.partition_key,
        )
        fo = io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=arrow_table.schema, **parquet_writer_kwargs) as writer:
                writer.write(arrow_table, row_group_size=row_group_size)
        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=writer.writer.metadata,
            stats_columns=compute_statistics_plan(file_schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(file_schema),
        )
        data_file = DataFile(
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