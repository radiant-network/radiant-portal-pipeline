from pydantic import BaseModel
from typing import Any
class PartitionCommit(BaseModel):
    parquet_files: list[str]
    partition_filter: dict[str, Any]



