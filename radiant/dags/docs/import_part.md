# Importing a partition in Radiant

Processes exactly one partition: VCF extraction into Iceberg, then the load into StarRocks.
Triggered once per partition by **radiant-import**.

---

## One run at a time

Three mechanisms, at three different levels. Only the first is a guarantee.

| Mechanism | Lives on | Covers | Job |
|:--|:--|:--|:--|
| **max_active_runs=1** | This DAG | Every run, whatever started it | The constraint |
| **import_part pool**, 1 slot | The trigger task in **radiant-import** | Only runs started by **radiant-import** | Ordering |
| **import_mutex** S3 lock | This DAG's first and last task | Every run, plus the re-annotation DAG | The backstop |

### The lock

Taken by **acquire_import_lock**, released by **release_import_lock**. It is a conditional
PUT (If-None-Match), so two concurrent runs cannot both get it — the loser's PUT comes back
412 and the task fails with **LockHeldError**, naming the holder.

It is also the only one of the three that excludes the *re-annotation* DAG, which holds the
same lock for the length of its run.

**The release is an Airflow teardown of the acquire, so a failed partition still gives the
lock back.** One bad partition would otherwise leave the mutex held and take every partition
behind it down too — a poor trade for a DAG that runs once per partition and fails routinely.

> The re-annotation DAG does the opposite and holds its lock on failure. It is one long run
> whose half-finished state an operator needs to look at before anything else writes.

Being a teardown, and not a plain ALL_DONE task, matters for the run state: Airflow leaves
teardowns out of it, so a failed task still fails the run, and **radiant-import** stops
instead of moving on to the next partition. As a plain ALL_DONE leaf, the release succeeded
after any failure and the run ended in success (RAD-133).

The teardown runs only if **acquire_import_lock** succeeded, so a run that lost the race does
not touch the winner's mutex. The release also passes its own holder string and deletes
nothing unless this run is the current holder. Clearing a failed task clears the acquire and
the release with it, so the retry takes the lock again before it writes.

Clearing a lock nobody will release is still an operator action: the toolbox DAG's
**check-lock** command, re-run with **-delete-if-expired** once past the 6h TTL.

### max_active_runs

The direct constraint, and the reason correctness no longer depends on pool configuration.
Holds however the run was started — from **radiant-import**, by hand, or through the API.

A second partition does not fail; it waits:

1. Its trigger creates the run with state **QUEUED**.
2. The scheduler declines to promote it while another run is active.
3. The trigger keeps waiting — it polls for success or failure, and QUEUED is neither.
4. The first run finishes, the queued run is promoted, and its **acquire_import_lock**
   succeeds because the first run released.

### The pool

It limits how many **triggers** run at once, not how many **radiant-import-part** runs exist,
so it does nothing for a run started outside **radiant-import**. That is why it is not the
guarantee.

It is kept for a job **max_active_runs** cannot do: **ordering**. Queued runs are promoted in
execution_date order — the instant each trigger happened to run. Firing the triggers one at a
time is what makes that order match the priority order **assign_priority** computed. Fire
them all at once and the timestamps land milliseconds apart in whatever order the workers got
there.

A trigger waiting on a QUEUED run has no deadline of its own, so it carries an
execution_timeout equal to the lock's staleness threshold. That fails the *waiter*, not the
wedged partition — the triggered run keeps going, and keeps the lock, until an operator acts.

> **That pool needs "Include deferred tasks" enabled.**
>
> TriggerDagRunOperator takes its **deferrable** default from **default_deferrable** in the
> operators section of the Airflow config. Where that is on, the trigger defers while it
> waits. Airflow's EXECUTION_STATES is RUNNING and QUEUED only — a deferred task holds no
> pool slot — so the slot is released the moment the trigger starts waiting, and the next
> partition fires immediately.
>
> That is [apache/airflow#40528](https://github.com/apache/airflow/issues/40528), still open.
> **include_deferred** on the pool is the workaround, and the same flag is required on
> **starrocks_insert_pool** for the same reason.

Without the flag the pool silently stops serialising, which is how two partitions end up
racing the lock.

---

## Why it matters

Concurrent partitions are not merely wasteful:

- **snv_variant** and **snv_consequence** are written by both the germline and somatic flows.
- **radiant-import-snv-vcf** funnels both through a single **merge_commits** into
  **commit_partitions**, so there is exactly one committer per part.
- Two partitions importing at once puts two committers on those Iceberg tables.

See design/SJRA-1751-snv-vcf-ingestion-fan-out.md.

---

## CNV frequencies

**aggregate_germline_cnv_variant_freq** and **aggregate_somatic_cnv_variant_freq** run after
the CNV occurrence insert of their group, once per tenant of the part, and are skipped with it
when the part has no CNV task (deleted tasks included, so a deletion also recomputes).

Each one rebuilds the whole **germline\_\_cnv\_\_variant\_frequency** or
**somatic\_\_cnv\_\_variant\_frequency** table of the tenant, over every part, with one
**INSERT OVERWRITE**:

- Two CNVs match when they are on the same chromosome, in the same class (**GAIN** and
  **GAINLOH** pooled) and their overlap covers at least 80% of each one's length.
- There is no per-part staging table, unlike the SNV frequencies: a patient of one part can
  match an interval of another, so a part cannot be counted alone.
- Runs are serial (see above), so two recomputes never write the same table at once.

See design/RAD-120-cnv-cohort-frequency.md.
