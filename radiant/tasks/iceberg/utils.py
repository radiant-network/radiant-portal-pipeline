import logging
import time
from pyiceberg.schema import Schema

from radiant.tasks.iceberg.partition_commit import PartitionCommit
from pyiceberg.catalog import Table
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo

logger = logging.getLogger("airflow.task")


def merge_schemas(schema1: Schema, schema2: Schema):
    """
    Merge two Iceberg schemas into a single schema.
    """
    appended_fields = list(schema1.fields) + list(schema2.fields)
    return Schema(*appended_fields)



def commit_files(table:Table, partition_to_commit: list[PartitionCommit]):
        """
        Commit all written Parquet files to the Iceberg table using overwrite mode.

        Raises:
            ValueError: If the partition filter is empty.
            CommitFailedException: If commit retries are exhausted.
        """
        if not partition_to_commit:
            logger.info(
                f"No partitions to commit for table {table.name()} partition filter"
            )
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
                raise ValueError(f"Partition filter {partition.partition_filter} must contain at least one key-value pair.")
            # tx.delete(filter_expr)
            if partition.parquet_files:
                tx.add_files(partition.parquet_files)

        max_retries = 20
        while max_retries > 0:
            try:
                tx.commit_transaction()
                logger.info(f"Successfully overwrite partitions")
                break
            except CommitFailedException as e:
                max_retries -= 1
                logger.info(f"Commit failed: {e}. Retries left: {max_retries}")
                time.sleep(1)
                if max_retries <= 0:
                    logger.error("Failed after 10 retries. Giving up.")
                    raise e