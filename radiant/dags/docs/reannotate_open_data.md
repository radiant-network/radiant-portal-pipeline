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
  → P2  reannotate_accumulators   snv__staging_variant · snv__consequence   (upsert in place)
  → P3a snv_variant chain         snv__variant → snv__variant_partitioned
        snv_consequence chain     snv__consequence_filter → …_partitioned
  → P3b cnv_occurrence            germline then somatic, per tenant × part
  → CHECKPOINT: every rebuild done
  → P4  record_open_data_release
  → release_import_lock
```

Inside P3a the two chains are independent of each other, but each chain is serial: a partitioned table is
a partitioned copy of the unpartitioned one above it, so the copy has to be rebuilt first.

P3b waits on `snv__variant` — both CNV statements join it to count the quality-passing SNVs inside each
segment (`nb_snv`), and P3a rebuilds that table with `INSERT OVERWRITE`. It does not wait on the
consequence chain, which reads nothing the CNV statements touch.

## Mutual exclusion with the import

The whole run holds the `import_mutex` S3 lock that `import_part` also takes. This is why P1–P4 live in
one DAG: an Airflow pool releases its slot when a *task* ends, so only a lock held for the length of the
run can keep `import_part` out of the middle of a re-annotation.

The lock is **not** released on failure — a failed run leaves it held, deliberately. Clearing it is an
operator action: run the toolbox DAG's `check-lock` command to see the holder and age, and re-run it with
`args=["-delete-if-expired"]` once the lock is past its 6h TTL.

## Which release did it run against?

P4 writes one row per open-data source to `open_data_release`, but only once every rebuild above it
succeeded — so a row there means the portal-facing tables really were rebuilt against that release.

Each row names the schema the source was **actually** read from, resolved the same way the statements
themselves resolve it (`radiant.tasks.data.open_data.build_open_data_release_rows`). A source held back
by `RADIANT_OPEN_DATA_USE_LEGACY_TABLES` is recorded against the Radiant Iceberg catalog under its
pre-contract `table_name`, with `iceberg_ref` and `dataset_version` NULL — it is read without time
travel, so there is no release to name. Stamping the OpenDataLake catalog and ref onto those rows would
claim a release the refresh never saw.

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

P1 triggers `radiant-import-open-data` with no conf, so that DAG's two file-driven broker loads — ClinVar
RCV summary and cytoband — skip. Neither source comes from OpenDataLake; both are file drops and stay a
manual, operator-triggered run of `radiant-import-open-data` with the paths filled in.
