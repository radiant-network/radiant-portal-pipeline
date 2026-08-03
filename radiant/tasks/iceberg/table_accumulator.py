import itertools
import logging
import os
import uuid

import pyarrow as pa
from pyiceberg.catalog import Table
from pyiceberg.io.pyarrow import (
    PyArrowFileIO,
)

from radiant.tasks.iceberg.utils import dataframe_to_data_files

logger = logging.getLogger("airflow.task")

PARQUET_FILE_SIZE_MB = 500  # Keep this value slightly lower than size of
# TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
# otherwise there is a risk to write small parquet files.
MAX_BUFFERED_ROWS = 10000


def resolve_parquet_file_size_mb() -> int:
    """Resolve the buffer flush threshold, overridable via ``RADIANT_PARQUET_FILE_SIZE_MB``.

    The threshold sets the memory floor for an extraction task, independently of how small the
    VCF is: a buffer flushes only once it reaches this size, ``merge_table`` briefly holds both
    the old and the concatenated copy, and three buffers are live at once (occurrence, variant,
    consequence). At the 500MB default that is several GB, which is more than a local
    single-node cluster can give each of several mapped writers.

    Lower it *only* for local/dev runs. The default is deliberately just under Iceberg's
    ``WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT`` (see above), so reducing it in a real deployment
    trades memory for a lot of small parquet files.

    An empty value counts as unset: the KubernetesPodOperator propagates env vars it was given
    as ``None``, and the container then sees an empty string rather than nothing at all.
    """
    raw = os.getenv("RADIANT_PARQUET_FILE_SIZE_MB")
    if not raw:
        return PARQUET_FILE_SIZE_MB
    return int(raw.strip().strip("\"'"))


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
        partition_filter: dict = None,
        max_buffered_rows: int = MAX_BUFFERED_ROWS,
        parquet_file_size_mb: int | None = None,
    ):
        """
        Initialize the TableAccumulator.

        Args:
            table (Table): The target Iceberg table instance.
            partition_filter (dict, optional): Partition key-value pairs to overwrite.
            max_buffered_rows (int): Maximum number of rows to buffer before merging.
            parquet_file_size_mb (int, optional): Parquet file size threshold in MB for writing.
                Defaults to :func:`resolve_parquet_file_size_mb`, i.e. ``RADIANT_PARQUET_FILE_SIZE_MB``
                if set, otherwise ``PARQUET_FILE_SIZE_MB``. Resolved here rather than as a default
                argument so the env var is read per instance instead of once at import.
        """
        self.table = table
        self.schema = self.remove_field_ids(table.schema().as_arrow())
        self.partition_filter = partition_filter or {}
        self.parquet_paths = []
        self.rows = []
        self.accumulated_pa_table = None
        self.max_buffered_rows = max_buffered_rows
        self.parquet_file_size_mb = (
            parquet_file_size_mb if parquet_file_size_mb is not None else resolve_parquet_file_size_mb()
        )

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
                    keys_sorted=field_type.keys_sorted,
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
        # gc.collect()

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
                self.write_files(merge=False)

    def write_files(self, merge: bool = True):
        """
        Write the accumulated Arrow table to a Parquet file

        Args:
            merge (bool): Whether to force a merge before writing.

        """
        logger.info("Write arrow to parquet")
        if merge:
            self.merge_table(force=True)

        if self.accumulated_pa_table:
            file_io = PyArrowFileIO(self.table.catalog.properties)
            counter = itertools.count()
            uuid_ = uuid.uuid4()
            files = list(
                dataframe_to_data_files(self.table.metadata, self.accumulated_pa_table, file_io, uuid_, counter)
            )
            logger.info(f"Writing {len(files)} files to parquet")
            logger.info(f"files {files}")
            self.parquet_paths.extend([f.file_path.replace("s3a", "s3") for f in files])
            self.accumulated_pa_table = None
