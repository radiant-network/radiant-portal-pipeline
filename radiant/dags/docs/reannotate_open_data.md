# Radiant — Re-annotate against OpenDataLake

Weekly refresh of the open-data reference tables, followed by a re-annotation of everything
derived from them. Design: design/SJRA-1811-opendatalake-integration.md, sections 4 and 5.

> **Manual trigger only, for now.** Every phase is wired, but the schedule is off. Flip the
> **schedule** argument in reannotate_open_data.py to Saturday 00:00 and drop the **manual**
> tag to start it. Once weekly, a missed or failed run is not re-run — the next week catches
> up.

---

## Flow

| # | Phase | Step | What it does |
|:--|:--|:--|:--|
| 1 | | **preflight_tables_exist** | Fails early if a source or target table is missing |
| 2 | | **acquire_import_lock** | Takes the import mutex for the whole run |
| 3 | P1 | **reference_load** | Triggers radiant-import-open-data, waits for it |
| | | *CHECKPOINT* | All sources loaded |
| 4 | P2 | **reannotate_accumulators** | snv\_\_staging_variant, then snv\_\_consequence. Upsert in place |
| | | *CHECKPOINT* | Accumulators re-annotated |
| 5 | P3a | **snv_variant** | snv\_\_variant into its partitioned copy |
| | | *CHECKPOINT* | SNV variants rebuilt |
| 6 | P3a | **snv_consequence** | snv\_\_consequence_filter into its partitioned copy |
| | | *CHECKPOINT* | SNV consequences rebuilt |
| 7 | P3b | **cnv_occurrence** | Germline then somatic, per tenant and part |
| | | *CHECKPOINT* | Every rebuild done |
| 8 | P4 | **record_open_data_release** | Stamps one row per source |
| 9 | | **release_import_lock** | Gives the mutex back |

A checkpoint sits between every group of StarRocks work. They run nothing — they are there so
the graph reads as the serial sequence it is, with each group bracketed by a marker rather
than the reader having to trace edges between two fan-outs to see where one operation ends
and the next begins.

### Everything here is serial

**There is no parallelism anywhere in the chain above.** Each statement is a whole-table scan
rather than a batch, so two at once contend for the same disk and spill budget, and the
contention costs more than the overlap wins.

The steps above order the **groups**. Inside a group, a tenant/part fan-out is N statements
that no edge separates — those are held apart by the pool below, not by
max_active_tis_per_dagrun, which cannot work here.

Only some of that order is a data dependency:

- Each P3a chain reads the accumulator above it.
- A partitioned table is a partitioned copy of the unpartitioned one above it.
- P3b joins snv\_\_variant to count the quality-passing SNVs in each segment (**nb_snv**),
  which P3a rebuilds with INSERT OVERWRITE.

The rest is deliberate serialisation and can be reordered, as long as nothing ends up running
two statements at once.

---

## Required setup: the starrocks_insert_pool

> **This DAG does not run correctly without a pool named starrocks_insert_pool, with exactly
> 1 slot and "Include deferred tasks" enabled.**

The pool is a workaround, not the natural tool. Airflow's concurrency limits do not count
deferred tasks — EXECUTION_STATES is RUNNING and QUEUED only — and these operators SUBMIT
TASK and then defer. So a statement stops being counted the moment it actually starts
running, and the scheduler releases the next one.

That is [apache/airflow#40528](https://github.com/apache/airflow/issues/40528), still open.
The reporter names a pool with **include_deferred** as the workaround, which is what this DAG
uses. If the issue is ever fixed, max_active_tasks on the DAG becomes the simpler way to say
this.

---

## Mutual exclusion with the import

The whole run holds the **import_mutex** S3 lock that **radiant-import-part** also takes.
This is why P1 through P4 live in one DAG: an Airflow pool releases its slot when a *task*
ends, so only a lock held for the length of the run can keep the import out of the middle of
a re-annotation.

| | This DAG | radiant-import-part |
|:--|:--|:--|
| Releases on | P4 success | ALL_DONE |
| After a failure | **Lock stays held** | Lock is given back |
| Why | One long run; an operator should see the half-finished state before anything else writes | Runs once per partition and fails routinely; holding on would block every partition behind it |

Clearing a held lock is an operator action: run the toolbox DAG's **check-lock** command to
see the holder and age, then re-run it with **-delete-if-expired** once past the 6h TTL.

---

## Which release did it run against?

P4 writes one row per open-data source to **open_data_release**, once no rebuild above it has
*failed*.

> **Note the gap.** **rebuilds_complete** and P4 are both NONE_FAILED, which treats a
> *skipped* branch as passing. Where tenant or part discovery short-circuits, the rebuilds
> skip and P4 still stamps a release row — so a row means "nothing failed", not "everything
> was rebuilt". Section 4 of the design asks for the stricter reading; the trigger rules here
> do not implement it yet.

Each row names the schema the source was **actually** read from, resolved the same way the
statements themselves resolve it (radiant.tasks.data.open_data.build_open_data_release_rows).

| Column | Contents |
|:--|:--|
| **source_name** | The primary key. See below |
| **table_name** | The table as actually read |
| **catalog_name**, **database_name** | The schema it was read from |
| **iceberg_ref** | The pinned ref, or the literal LEGACY |
| **dataset_version** | Set only when a concrete release is pinned. See below |
| **recorded_at** | NOW(), evaluated by StarRocks when P4 executes |

A few of those need explaining:

- **A legacy source is recorded honestly.** One held back by
  **RADIANT_OPEN_DATA_USE_LEGACY_TABLES** is recorded against the Radiant Iceberg catalog
  under its pre-contract table name, with **iceberg_ref** and **dataset_version** both set to
  the literal LEGACY. It is read without time travel, so there is no release to name, and
  LEGACY says so rather than leaving a NULL that reads like "unknown". Stamping the
  OpenDataLake catalog and ref onto those rows would claim a release the refresh never saw.
- **The key is the source, not the table.** It is a PRIMARY KEY table keyed on
  **source_name**, so it always shows the current state: the latest run wins per source, and
  a retried P4 upserts rather than adding a second copy. No release history is kept here. The
  table name is exactly what changes when a source flips from its pre-contract name to the
  versioned contract name, so keying on it would leave the old name behind as a stale second
  row.
- **recorded_at is the end, not the start.** It is the moment the rebuilds finished, not the
  moment the statement was built. The rows and the statement are assembled at the top of the
  run — they need nothing the rebuilds produce, and building them early surfaces a config
  error before the expensive phases — which is hours earlier.
- **dataset_version is often NULL.** It is filled only when **RADIANT_OPEN_DATA_REF** pins a
  concrete OpenDataLake release. On the default "latest" it stays NULL: latest is a tag that
  moves with each publish, and resolving it back to a dataset version needs Iceberg ref
  metadata this pipeline has no client for.

---

## Parameters

None. Tenants and parts are discovered from **staging_sequencing_experiment** at run time
(radiant.tasks.data.tenants), so the fan-out follows the data rather than a hardcoded list.

### What P1 passes on

P1 triggers radiant-import-open-data with **skip_legacy_tables** set to true, and nothing
else. That flag makes the import skip every source still read from the legacy Radiant Iceberg
catalog:

- The ones held back by **RADIANT_OPEN_DATA_USE_LEGACY_TABLES**.
- The two with no upstream contract at all: **ensembl_gene** and **ensembl_exon_by_gene**.

None of them move when OpenDataLake publishes, so re-importing them here is work with no new
data behind it. A manual run of radiant-import-open-data leaves the flag false and imports
everything, as before.

Because nothing else is passed, that DAG's file-driven branches — the ClinVar RCV summary and
cytoband broker loads, and the COSMIC gene set import it triggers — still skip: all are gated on
filepath params P1 does not set. None of them
comes from OpenDataLake; they are file drops and stay a manual, operator-triggered run with
the paths filled in.

> **On an unmigrated environment, P1 is a no-op.**
> **RADIANT_OPEN_DATA_USE_LEGACY_TABLES** defaults to the wildcard, so *every* source is held
> back. That is the honest answer — there is no OpenDataLake release to re-annotate against —
> but it means such a run finishes quickly and rebuilds against unchanged reference data.
