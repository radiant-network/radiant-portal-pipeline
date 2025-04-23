import os
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Table
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo, And
import logging

logger = logging.getLogger(__name__)

PARQUET_FILE_SIZE_MB = 256
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
    def remove_field_ids(schema: pa.Schema):
        """
        Return a new Arrow schema with field IDs removed.

        Args:
            schema (pa.Schema): An Arrow schema that may include Iceberg field IDs.

        Returns:
            pa.Schema: A clean schema without field IDs.
        """
        fields = []
        for field in schema:
            pa_field = pa.field(field.name, field.type, nullable=field.nullable)
            fields.append(pa_field)
        return pa.schema(fields)

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
                self.write_files(commit=False, merge=False)

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
        pq.write_table(self.accumulated_pa_table, parquet_path, filesystem=self.fs)
        self.accumulated_pa_table = None
        if commit:
            self.commit_files()
        return parquet_path

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
        max_retries = 20
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
                    logger.error(f"Failed after 10 retries. Giving up.")
                    raise e
