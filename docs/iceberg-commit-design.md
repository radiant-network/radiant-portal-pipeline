# Iceberg Commit Performance — Design Guide

## Context

The Radiant ETL writes genomic VCF data into Iceberg tables via PyIceberg.
Two DAGs run concurrently and contend for shared tables:

- `import-germline-snv-vcf` → `germline_snv_occurrence`, `snv_variant`, `snv_consequence`
- `import-somatic-snv-vcf` → `somatic_snv_occurrence`, `snv_variant`, `snv_consequence`

`snv_variant` and `snv_consequence` are written by **both** pipelines. When
both DAGs commit at the same time, commit duration grows from seconds to
many minutes due to retries on `CommitFailedException`.

This document inventories the bottlenecks and proposes fixes in priority
order.

---

## Current Architecture

### Write path

1. Each VCF processing task (`radiant/tasks/vcf/snv/{germline,somatic}/process.py`)
   parses a VCF and feeds rows into a `TableAccumulator`
   (`radiant/tasks/iceberg/table_accumulator.py`).
2. `TableAccumulator` buffers rows (`MAX_BUFFERED_ROWS = 10000`), flushes
   to a PyArrow table when memory crosses `PARQUET_FILE_SIZE_MB = 500`, and
   writes parquet files via `dataframe_to_data_files`
   (`radiant/tasks/iceberg/utils.py:88`).
3. `dataframe_to_data_files` produces full `DataFile` objects with
   pre-computed statistics (lower/upper bounds, null counts, value counts)
   from the parquet writer metadata.
4. `TableAccumulator.write_files` keeps **only** `file_path` strings — the
   `DataFile` objects with stats are discarded.
5. Each task returns a `PartitionCommit` (`parquet_files: list[str]`,
   `partition_filter: dict`) per table.

### Commit path

`commit_files` (`radiant/tasks/iceberg/utils.py:45`):

```python
tx = table.transaction()
for partition in partition_to_commit:
    filter_expr = And(EqualTo(col, val) for col, val in partition.partition_filter.items())
    tx.delete(filter_expr)
    tx.add_files(partition.parquet_files)

# 20 retries on CommitFailedException, fixed sleep(1)
tx.commit_transaction()
```

`tx.add_files(paths)` re-opens every parquet file from S3 to read its
footer and rebuild stats — the same stats already computed at write time.

---

## Bottlenecks

### 1. Redundant parquet metadata reads

`tx.add_files` performs one S3 GET per file to read the parquet footer
and recompute statistics. With N files per commit, this is N S3 round
trips on the critical path. The stats already exist as `DataFile`
objects from `_write_file` (`radiant/tasks/iceberg/utils.py:155`); they
are silently dropped at line 180.

### 2. Optimistic-concurrency collisions on shared tables

PyIceberg uses optimistic concurrency control: each transaction reads
metadata at start, builds a new snapshot, and atomically swaps the
metadata pointer. If another writer commits in between, the swap fails
with `CommitFailedException`. Both germline and somatic commit
`snv_variant` and `snv_consequence` simultaneously; one always loses.

### 3. Retry without jitter

`utils.py:73-85` sleeps a fixed `time.sleep(1)` between retries. When
two writers collide, they both wake up at the same time, retry at the
same time, collide again. Classic thundering herd. Up to 20 attempts
before failure.

### 4. Two-phase partition replace

`tx.delete(filter_expr) + tx.add_files(paths)` runs as two operations
inside one transaction. The delete triggers manifest scanning to find
files that match the predicate, builds positional/equality deletes,
then add_files appends new files. Two passes over manifest state.

### 5. No commit-level concurrency control

The Airflow `import_vcf` pool gates parquet writes but commit tasks
run unbounded. Nothing prevents concurrent commits to the same table.

### 6. Manifest list growth

No scheduled `expire_snapshots` or `rewrite_manifests`. Over time the
manifest list grows; commit planning has to scan more manifests on
every transaction. Slowdown is gradual but compounding.

### 7. Default PyIceberg worker count

`PYICEBERG_MAX_WORKERS` defaults to 4. This caps parallelism for
parquet metadata reads and manifest writes inside a transaction.

---

## Fixes (Priority Order)

### P0 — Airflow pool for shared-table commits

**Effort:** 10 minutes. **Risk:** none.

Add an Airflow pool `iceberg-snv-shared-commit` with `slots=1`. Apply it
to the commit tasks that touch `snv_variant` and `snv_consequence`.
Germline and somatic commits queue instead of colliding. Parquet writes
remain fully parallel.

```python
commit_partitions_task = PythonOperator(
    task_id="commit_partitions",
    python_callable=commit_partitions,
    pool="iceberg-snv-shared-commit",
)
```

This converts a multi-minute retry storm into back-to-back fast commits.

### P0 — Retry with jitter and exponential backoff

**Effort:** 15 minutes. **Risk:** low.

Replace fixed sleep with exponential backoff plus jitter:

```python
import random

attempt = 0
max_retries = 20
while attempt < max_retries:
    try:
        tx.commit_transaction()
        break
    except CommitFailedException as e:
        attempt += 1
        if attempt >= max_retries:
            logger.error(f"Commit failed after {max_retries} attempts")
            raise
        backoff = min(60.0, (2 ** attempt) * 0.1) + random.uniform(0, 1)
        logger.info(f"Commit failed: {e}. Retry {attempt}/{max_retries} in {backoff:.1f}s")
        time.sleep(backoff)
```

Stops lockstep retries. Mandatory if Airflow pool is not used.

### P1 — Manifest tuning + scheduled maintenance

**Effort:** half a day. **Risk:** low.

Set Iceberg table properties at init:

```
commit.manifest-merge.enabled = true
commit.manifest.target-size-bytes = 8388608
commit.manifest.min-count-to-merge = 100
```

Reference: [Iceberg table properties](https://iceberg.apache.org/docs/latest/configuration/#table-properties).

Add a maintenance DAG that runs weekly:

- `expire_snapshots` retaining last 7 days
- `rewrite_manifests` to consolidate manifest count
- Optional: `rewrite_data_files` for compaction

Reference: [PyIceberg maintenance API](https://py.iceberg.apache.org/api/#maintenance-operations).

Without maintenance, commit planning time grows linearly with snapshot
count.

### P1 — Bump PyIceberg worker count

**Effort:** 5 minutes. **Risk:** none.

Set `PYICEBERG_MAX_WORKERS=32` in the operator environment. Parallelizes
parquet stat reads and manifest I/O. Cheap mitigation while the
metadata-reread issue (P2) is being fixed.

Reference: [PyIceberg concurrency config](https://py.iceberg.apache.org/configuration/#concurrency).

### P2 — Eliminate parquet metadata re-reads

**Effort:** 2-3 days. **Risk:** medium (touches XCom serialization).

Carry full `DataFile` objects from write to commit. Replace
`tx.delete(filter) + tx.add_files(paths)` with `OverwriteFiles` using
pre-built data files.

**Step 1 — Keep DataFile objects in TableAccumulator:**

```python
# radiant/tasks/iceberg/table_accumulator.py
self.data_files: list[DataFile] = []

def write_files(self, merge: bool = True):
    if merge:
        self.merge_table(force=True)
    if self.accumulated_pa_table:
        file_io = PyArrowFileIO(self.table.catalog.properties)
        files = list(dataframe_to_data_files(
            self.table.metadata, self.accumulated_pa_table, file_io,
            uuid.uuid4(), itertools.count(),
        ))
        self.data_files.extend(files)
        self.accumulated_pa_table = None
```

**Step 2 — Serialize DataFile in PartitionCommit:**

PyIceberg `DataFile` is a Pydantic-compatible record. Persist as JSON via
its Avro/manifest serialization or a custom dump. Important: the file
path, partition tuple, file size, and statistics dict must all survive
the XCom round trip.

**Step 3 — Replace delete + add_files with overwrite_files:**

```python
# radiant/tasks/iceberg/utils.py
from pyiceberg.table import Transaction

def commit_files(table: Table, partition_to_commit: list[PartitionCommit]):
    if not partition_to_commit:
        return
    table.refresh()
    tx = table.transaction()
    for partition in partition_to_commit:
        filter_expr = _build_filter(partition.partition_filter)
        with tx.update_snapshot().overwrite(commit_uuid=uuid.uuid4()) as snap:
            snap.delete_by_predicate(filter_expr)  # or _OverwriteFiles overwrite_filter
            for data_file in partition.data_files:
                snap.append_data_file(data_file)
    tx.commit_transaction()  # wrap in retry loop with jitter
```

Result: no S3 GETs for parquet footers during commit. Commit time becomes
proportional to manifest writes only.

### P2 — Partition shared tables by analysis_type

**Effort:** table migration, 1-2 days plus rewrites. **Risk:** medium-high.

Add `analysis_type` (germline / somatic) to the partition spec of
`snv_variant` and `snv_consequence`. Germline writers touch only
`analysis_type=germline` partitions; somatic only `analysis_type=somatic`.
Optimistic-CC collisions become impossible because the manifests do not
overlap.

This is the root-cause fix for cross-DAG contention. Recommend doing it
**after** P0 and P1 ship and confirm whether contention is still a
problem.

### P3 — Increase row buffer

**Effort:** 1 line. **Risk:** memory pressure on large VCFs.

Bump `MAX_BUFFERED_ROWS` from 10000 to 100000 in
`radiant/tasks/iceberg/table_accumulator.py:18`. Reduces frequency of
`pa.concat_tables` calls during VCF processing. Minor win on the write
side, not the commit side. Validate memory headroom on large genomes
before raising.

---

## Implementation Plan

| Order | Change | Effort | Win |
|-------|--------|--------|-----|
| 1 | Airflow pool for shared-table commits | 10 min | Eliminates concurrent collisions |
| 2 | Retry with jitter + exponential backoff | 15 min | Stops thundering herd |
| 3 | Bump `PYICEBERG_MAX_WORKERS=32` | 5 min | Faster `add_files` |
| 4 | Manifest table properties + maintenance DAG | 1 day | Bounds manifest growth |
| 5 | Persist DataFile end-to-end + `OverwriteFiles` | 2-3 days | Skips footer re-reads |
| 6 | Repartition shared tables by `analysis_type` | 2 days | Root-cause fix |
| 7 | Bigger row buffer | 5 min | Minor write-side win |

Land 1-3 first. Re-measure commit duration. Decide whether 5 and 6 are
still needed.

---

## References

- [PyIceberg writes & commits](https://py.iceberg.apache.org/api/#write-support)
- [PyIceberg `add_files` source](https://github.com/apache/iceberg-python/blob/main/pyiceberg/table/__init__.py)
- [Iceberg table properties](https://iceberg.apache.org/docs/latest/configuration/#table-properties)
- [Iceberg optimistic concurrency model](https://iceberg.apache.org/docs/latest/reliability/)
- Source files:
  - `radiant/tasks/iceberg/utils.py` — `commit_files`, `dataframe_to_data_files`
  - `radiant/tasks/iceberg/table_accumulator.py` — `TableAccumulator`
  - `radiant/tasks/iceberg/partition_commit.py` — `PartitionCommit`
  - `radiant/tasks/vcf/snv/germline/process.py` — germline write path
  - `radiant/tasks/vcf/snv/somatic/process.py` — somatic write path
  - `radiant/dags/import_germline_snv_vcf.py` — germline DAG
