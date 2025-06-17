from typing import Any

from pydantic import BaseModel


class PartitionCommit(BaseModel):
    parquet_files: list[str]
    partition_filter: dict[str, Any]
