# Radiant — Re-annotate against OpenDataLake

Weekly refresh of the open-data reference tables, followed by a re-annotation of everything
derived from them. Design: design/SJRA-1811-opendatalake-integration.md, sections 4 and 5.

> **Scheduled weekly, Saturday 00:00.** A missed or failed run is not re-run — the next week
> catches up.

---

## Flow

| # | Phase | Step | What it does |
|:--|:--|:--|:--|
| 1 | | **preflight_tables_exist** | Fails early if a source or target table is missing |
| 2 | | **acquire_import_lock** | Takes the import mutex for the whole run |
| 3 | P1 | **reference_load** | Triggers radiant-import-open-data, waits for it |
| | | *CHECKPOINT* | All sources loaded |
| | | **compute_reannotation_gates** | Which branches have a source that moved. See below |
| 4 | P2 | **reannotate_accumulators** | snv\_\_staging_variant, then snv\_\_consequence. Upsert in place |
| | | *CHECKPOINT* | Accumulators re-annotated |
| 5 | P3a | **snv_variant** | snv\_\_variant into its partitioned copy |
| | | *CHECKPOINT* | SNV variants rebuilt |
| 6 | P3a | **snv_consequence** | snv\_\_consequence_filter into its partitioned copy |
| | | *CHECKPOINT* | SNV consequences rebuilt |
| 7 | P3b | **cnv_occurrence** | Germline then somatic, per tenant and part |
| | | *CHECKPOINT* | Every rebuild done |
| 8 | P4 | **record_open_data_release** | Promotes what P1 imported to annotated. One UPDATE |
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

## Only what moved gets re-annotated

**compute_reannotation_gates** runs right after P1 and decides, per branch, whether there is
anything new to annotate against. Each statement in a gated-out branch skips itself through
**skip_if**; the checkpoints are NONE_FAILED, so the rest of the chain carries on.

| Branch | Watches | Statements it gates |
|:--|:--|:--|
| **snv_variant** | 1000_genomes, clinvar, dbsnp, gnomad_joint, omim_gene_set, topmed_bravo | The staging-variant re-annotation and both snv\_\_variant inserts |
| **snv_consequence** | dbnsfp, gnomad_constraint, spliceai | The consequence re-annotation and both consequence-filter inserts |
| **cnv_occurrence** | ensembl_gene, gnomad_sv | Germline then somatic CNV occurrences |

The watch lists live in **REANNOTATION_SOURCES** in radiant.tasks.data.open_data and are copied
by hand from the FROM and JOIN lists of the statements. **Change a statement's joins and you have
to change that map** — nothing checks it for you, and a source dropped from it silently stops
gating.

A watched source only moves a gate if it has an OpenDataLake contract. A source **held back** with
RADIANT_OPEN_DATA_USE_LEGACY_TABLES is read straight off the legacy Radiant catalog, carries no ref
and no snapshot, and cannot open a gate while it is held — so the gate gets narrower as an
environment holds more back, never wrong. Cytoband cannot be watched at all: it is a broker load
with no Iceberg source anywhere. clinvar_rcv has a contract but no re-annotation statement reads
it, so it is deliberately absent.

### The signal is the Iceberg snapshot, not dataset_version

**dataset_version could never answer "did this change?"** — it was a restatement of the ref, NULL
whenever the ref is a moving tag.
What can is the snapshot the ref resolves to. StarRocks exposes it from 3.4.1 as an Iceberg
metadata table — **SELECT snapshot_id FROM the_relation$refs WHERE name = the_ref** — so this
needs no new client, just the starrocks_conn that was already there. P1 records that snapshot per
source, P4 promotes it, and the next run compares against it.

The gate runs **after** P1 on purpose: P1 is what refreshes the external metadata cache, and a
$refs read before it can still report last week's snapshot.

### The same read also names the release

OpenDataLake publishes each version onto **its own branch** and moves the **latest** tag onto the
newest one (radiant-open-datalake — WapLoader.publishVersionBranch, IcebergTable.LatestTag). So the
release a run read is the branch sharing a snapshot with the ref, and the one $refs read already on
the wire returns it. That is what fills **dataset_version**, on a moving tag as well as a pinned
one — so the ledger can answer "which dbSNP is the portal showing?" with GCF_000001405.40 rather
than a 64-bit snapshot id.

Two branch names are never the answer: **main**, which the loader leaves empty, and
**audit_&lt;version&gt;**, the staging branch it drops once the publish succeeds.

### It fails open

Anything the gate cannot establish counts as changed, and the branch runs:

- A source with no recorded row — the first run ever, or one added to the mapping since.
- A contract source whose $refs read raises or returns nothing. A StarRocks older than 3.4.1 has
  no $refs at all; the gate logs the failure and the branch rebuilds.
- Every source, when open_data_release itself cannot be read — an environment that has not taken
  the migration below. The gates then do nothing, which is what these DAGs did before they existed.
  The ledger writes still fail on the missing columns, so the migration cannot be skipped quietly.
- A source that just migrated to OpenDataLake, or was just rolled back to legacy. Both replace
  values as surely as a publish does.

A **held back** source is the one case treated as never changing. It is read without time travel
and no refresh touches it, so reporting it as moved every week would defeat the gate entirely on
an unmigrated environment.

### P1 gates too, on the other column

**import-open-data** carries the same kind of gate on its own inserts, so a source that has not
published is not re-read into StarRocks either. Its override is **force_import** — set it after
editing an insert statement, and after truncating or recreating a StarRocks open-data table,
neither of which a snapshot comparison can see.

One table, **two snapshot columns**, because the two values drift apart:

| Column | Records | Written by |
|:--|:--|:--|
| **imported_snapshot_id** | What the StarRocks open-data copies hold | The import, at the end of its run |
| **reannotated_snapshot_id** | What the portal-facing tables were annotated from | P4, by promoting the column above |

A standalone import moves the first without the second, so neither column alone answers both
questions. Gating imports on reannotated_snapshot_id would leave a standalone run comparing against a value
nothing had written, re-reading the same snapshot forever; gating re-annotation on
imported_snapshot_id would skip a rebuild the warehouse still needs.

P4 is now one UPDATE — **SET reannotated_snapshot_id = imported_snapshot_id** — not a fresh $refs read. What the
rebuilds annotated is whatever P1 loaded, so re-resolving the ref hours later would stamp a publish
that landed mid-run as annotated, and the following run would skip the rebuild that publish needs.
The snapshot is resolved exactly once per import, by compute_import_gates, and those are the values
the import records.

### A variant rebuild always drags the CNV rebuild with it

**nb_snv** counts rows in snv\_\_variant, which the variant chain rebuilds with INSERT OVERWRITE.
So the cnv_occurrence gate opens whenever the snv_variant gate does, even when no CNV source
moved — the same dependency the P3a-before-P3b ordering exists for.

### When to override it

> **The gate watches data, not code.** It cannot see that you edited a statement, added a column,
> or fixed a join. After any change to the re-annotation SQL, run with
> **force_reannotation** set to true, or the run will skip the very branch you changed.

### Required setup on an existing deployment: the snapshot-column migration

> **A database created before SJRA-1950 has neither snapshot column, and the ledger writes fail on
> them.** Run sql/radiant/migrations/SJRA-1950_open_data_release_add_snapshot_columns.sql once, against
> the base database only — open_data_release is not per-tenant.

New deployments get both columns from init/open_data_release_create_table.sql and must not run it.
The first run after the migration re-imports and re-annotates everything: the existing rows carry no
snapshot, and there is no way to recover which one they used. That run establishes the baseline, and
the one after it is the first that can skip anything.

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
| **imported_snapshot_id** | The Iceberg snapshot P1 loaded the StarRocks copy from |
| **reannotated_snapshot_id** | The same value once P4 promotes it. What the next run's re-annotation gate compares against |
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

One. **force_reannotation**, default false — rebuild every branch whether or not its sources
moved. Set it after editing a re-annotation statement; see the gate section above.

Tenants and parts take no parameter: they are discovered from **staging_sequencing_experiment**
at run time (radiant.tasks.data.tenants), so the fan-out follows the data rather than a
hardcoded list.

### What P1 passes on

P1 triggers radiant-import-open-data with **skip_legacy_tables** set to true, and nothing
else. That flag makes the import skip every source still read from the legacy Radiant Iceberg
catalog, i.e. the ones held back by **RADIANT_OPEN_DATA_USE_LEGACY_TABLES**. Every source now
has an OpenDataLake contract (the two ensembl tables were the last to get one), so on a fully
migrated environment nothing is skipped.

Held-back sources do not move when OpenDataLake publishes, so re-importing them here is work
with no new data behind it. A manual run of radiant-import-open-data leaves the flag false and
imports everything, as before.

Because nothing else is passed, that DAG's file-driven branches — the cytoband broker load and
the COSMIC gene set import it triggers — still skip: both are gated on filepath params P1 does
not set. Neither comes from OpenDataLake; they are file drops and stay a manual,
operator-triggered run with the paths filled in.

The ClinVar RCV summary is the one file drop that also has an OpenDataLake source
(**clinvar_rcv_v1**), so P1 does refresh it unless **clinvar_rcv** is held back. Its broker load
stays behind the same filepath param — the fallback when it is held back, an override when it is
not.

> **On an unmigrated environment, P1 is a no-op.**
> **RADIANT_OPEN_DATA_USE_LEGACY_TABLES** defaults to the wildcard, so *every* source is held
> back. That is the honest answer — there is no OpenDataLake release to re-annotate against —
> but it means such a run finishes quickly and rebuilds against unchanged reference data.
