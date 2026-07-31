from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel


class PartitionCommit(BaseModel):
    parquet_files: list[str]
    partition_filter: dict[str, Any]


def merge_partition_commits(partition_lists: Iterable[dict[str, list[Any]] | None]) -> dict[str, list[Any]]:
    """Concatenate per-task `{table_name: [PartitionCommit]}` dicts into one.

    Empty entries are skipped so that a flow with no tasks in the part contributes nothing.

    Deliberately here and not in `utils.py`: the caller is the `merge_commits` PyOp, which runs
    in the Airflow scheduler's interpreter, and that environment (`requirements-airflow.txt`,
    `mwaa/*/requirements-*.txt`) has neither pyiceberg nor pyarrow. Importing `utils` there
    would raise ModuleNotFoundError at task runtime. This module needs only pydantic, which the
    Airflow environment does have.
    """
    merged: dict[str, list[Any]] = defaultdict(list)
    for partitions in partition_lists:
        if not partitions:
            continue
        for table, partition_commits in partitions.items():
            merged[table].extend(partition_commits)
    return dict(merged)
