# Radiant — Re-annotate against OpenDataLake

Weekly refresh of the open-data reference tables, followed by a re-annotation of everything derived from
them. Design: `design/SJRA-1811-opendatalake-integration.md` (§4 and §5).

Designed to run **Saturday 00:00**, and currently held at **manual trigger only** — every phase is wired,
but the schedule is not turned on yet. Flip `schedule` in `reannotate_open_data.py` and drop the `manual`
tag to start it. Once weekly, a missed or failed run is not re-run: the next week catches up.

## Flow

```
preflight_tables_exist
  → acquire_import_lock
  → P1  reference_load            (triggers radiant-import-open-data, waits)
  → CHECKPOINT: all sources loaded
  → P2  reannotate_accumulators   snv__staging_variant → snv__consequence   (upsert in place)
  → CHECKPOINT: accumulators re-annotated
  → P3a snv_variant               snv__variant → snv__variant_partitioned
  → CHECKPOINT: SNV variants rebuilt
  → P3a snv_consequence           snv__consequence_filter → …_partitioned
  → CHECKPOINT: SNV consequences rebuilt
  → P3b cnv_occurrence            germline then somatic, per tenant × part
  → CHECKPOINT: every rebuild done
  → P4  record_open_data_release
  → release_import_lock
```

A checkpoint sits between every group of StarRocks work. They run nothing — they are there so the graph
reads as the serial sequence it is, with each group bracketed by a marker rather than the reader having
to trace edges between two fan-outs to see where one operation ends and the next begins.

**Every StarRocks statement in this DAG is serial — there is no parallelism anywhere in the arrow chain
above.** Each one is a whole-table scan rather than a batch, so two at once contend for the same disk and
spill budget on the cluster, and the contention costs more than the overlap wins. The edges above order
the *groups*; inside a group, a tenant/part fan-out is N statements that no edge separates, and those are
held apart by the pool below — not by `max_active_tis_per_dagrun`, which cannot work here (see why).

Only some of that order is a data dependency: each 3a chain reads the accumulator above it, a partitioned
table is a partitioned copy of the unpartitioned one above it, and P3b joins `snv__variant` to count the
quality-passing SNVs in each segment (`nb_snv`) — which P3a rebuilds with `INSERT OVERWRITE`. The rest is
deliberate serialisation and can be reordered, as long as nothing starts running two statements at once.

## Required setup: the `starrocks_insert_pool`

**This DAG does not run correctly without a pool named `starrocks_insert_pool`, with exactly 1 slot and
"Include deferred tasks" enabled.**

The pool is a workaround, not the natural tool. Airflow's concurrency limits do not count deferred tasks
— `EXECUTION_STATES` is `{RUNNING, QUEUED}` — and these operators `SUBMIT TASK` and then defer, so a
statement stops being counted the moment it actually starts running and the scheduler releases the next
one. That is [apache/airflow#40528](https://github.com/apache/airflow/issues/40528), still open; the
reporter names a pool with `include_deferred` as the workaround, which is what this DAG uses. If that
issue is ever fixed, `max_active_tasks` on the DAG becomes the simpler way to express this.

## Mutual exclusion with the import

The whole run holds the `import_mutex` S3 lock that `import_part` also takes. This is why P1–P4 live in
one DAG: an Airflow pool releases its slot when a *task* ends, so only a lock held for the length of the
run can keep `import_part` out of the middle of a re-annotation.

The lock is **not** released on failure — a failed run leaves it held, deliberately. Clearing it is an
operator action: run the toolbox DAG's `check-lock` command to see the holder and age, and re-run it with
`args=["-delete-if-expired"]` once the lock is past its 6h TTL.

## Which release did it run against?

P4 writes one row per open-data source to `open_data_release`, once no rebuild above it has *failed*.

Note the gap: `rebuilds_complete` and P4 are both `NONE_FAILED`, which treats a **skipped** branch as
passing. On a platform where tenant or part discovery short-circuits, the rebuilds skip and P4 still
stamps a release row — so a row means "nothing failed", not "everything was rebuilt". Design §4 asks for
the stricter reading (*"a failed or partially-skipped run does not release"*); the trigger rules here do
not implement it yet.

Each row names the schema the source was **actually** read from, resolved the same way the statements
themselves resolve it (`radiant.tasks.data.open_data.build_open_data_release_rows`). A source held back
by `RADIANT_OPEN_DATA_USE_LEGACY_TABLES` is recorded against the Radiant Iceberg catalog under its
pre-contract `table_name`, with `iceberg_ref` and `dataset_version` both set to the literal `LEGACY` — it
is read without time travel, so there is no release to name, and `LEGACY` says that explicitly rather
than leaving a NULL that reads like "unknown". Stamping the OpenDataLake catalog and ref onto those rows
would claim a release the refresh never saw.

It is a PRIMARY KEY table keyed on `source_name`, so it always shows the current state: the latest run
wins per source, and a retried P4 upserts rather than adding a second copy. No release history is kept
here. The key is the source rather than `table_name` because the table name is exactly what changes
when a source flips from its pre-contract name to `{source}_v{MAJOR}` — keying on it would leave the
old name behind as a stale second row.

`recorded_at` is `NOW()`, evaluated by StarRocks when P4 executes — so it is the moment the rebuilds
finished, not the moment the statement was built. The rows and the statement are assembled at the top of
the run (they need nothing the rebuilds produce, and building them early surfaces a config error before
the expensive phases), which is hours earlier.

`dataset_version` is filled only when `RADIANT_OPEN_DATA_REF` pins a concrete OpenDataLake release. On the
default `latest` it stays NULL: `latest` is a tag that moves with each publish, and resolving it back to a
`dataset_version` needs Iceberg ref metadata this pipeline has no client for.

## Parameters

None. Tenants and parts are discovered from `staging_sequencing_experiment` at run time
(`radiant.tasks.data.tenants`), so the fan-out follows the data rather than a hardcoded list.

P1 triggers `radiant-import-open-data` with `conf={"skip_legacy_tables": True}` and nothing else. That
flag makes the import skip every source still read from the legacy Radiant Iceberg catalog — the ones
held back by `RADIANT_OPEN_DATA_USE_LEGACY_TABLES`, plus the three with no upstream contract at all
(`ensembl_gene`, `ensembl_exon_by_gene`, `cosmic_gene_panel`). None of them move when OpenDataLake
publishes, so re-importing them here is work with no new data behind it. A manual run of
`radiant-import-open-data` leaves the flag `False` and imports everything, as before.

Because nothing else is passed, that DAG's two file-driven broker loads — ClinVar RCV summary and
cytoband — still skip: both are gated on filepath params P1 does not set. Neither source comes from
OpenDataLake; both are file drops and stay a manual, operator-triggered run with the paths filled in.

Worth knowing: `RADIANT_OPEN_DATA_USE_LEGACY_TABLES` defaults to `*`, so on an environment that has not
been migrated yet **every** source is held back and P1 becomes a no-op. That is the honest answer — there
is no OpenDataLake release to re-annotate against — but it means such a run finishes quickly and rebuilds
against unchanged reference data.
