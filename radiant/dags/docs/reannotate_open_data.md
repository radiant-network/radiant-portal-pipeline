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

> Atomic on every stack, the local one included: MinIO enforces the `If-None-Match: *` precondition
> `acquire_lock` sends from `RELEASE.2024-09-13T20-26-02Z`, older than the images this repo pins.

The whole run holds the `import_mutex` S3 lock that `import_part` also takes. This is why P1–P4 live in
one DAG: an Airflow pool releases its slot when a *task* ends, so only a lock held for the length of the
run can keep `import_part` out of the middle of a re-annotation.

The lock is **not** released on failure — a failed run leaves it held, deliberately. Clearing it is an
operator action: run the toolbox DAG's `check-lock` command to see the holder and age, and re-run it with
`args=["-delete-if-expired"]` once the lock is past its 6h TTL.

## Which release did it run against?

P4 writes one row per contract table to `open_data_release`, but only once every rebuild above it
succeeded — so a row there means the portal-facing tables really were rebuilt against that release.

It is a PRIMARY KEY table keyed on `table_name`, so it always shows the current state: the latest run
wins per table, and a retried P4 upserts rather than adding a second copy. No release history is kept
here.

`dataset_version` is filled only when `RADIANT_OPEN_DATA_REF` pins a concrete OpenDataLake release. On the
default `latest` it stays NULL: `latest` is a tag that moves with each publish, and resolving it back to a
`dataset_version` needs Iceberg ref metadata this pipeline has no client for.

## Parameters

None. Tenants and parts are discovered from `staging_sequencing_experiment` at run time
(`radiant.tasks.data.tenants`), so the fan-out follows the data rather than a hardcoded list.

P1 triggers `radiant-import-open-data` with no conf, so that DAG's two file-driven broker loads — ClinVar
RCV summary and cytoband — skip. Neither source comes from OpenDataLake; both are file drops and stay a
manual, operator-triggered run of `radiant-import-open-data` with the paths filled in.
