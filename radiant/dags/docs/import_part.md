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

## Germline CNV sets

A sequencing can hold two germline CNV sets, each under its own task:

| Set | Task | Members | Extra columns |
|:--|:--|:--|:--|
| DRAGEN | **alignment_germline_variant_calling** | every member | none |
| slivar | **radiant_germline_cnv_annotation** | the proband only | parental origin, transmission mode, the parents' calls, the CNV Exomiser result |

- The slivar set holds the proband's carrier records of the family's slivar VCF. A **0/0** or
  **./.** record of the proband gives no row.
- On the slivar set, **name** is the proband's own DRAGEN call ID (FORMAT/OID), the same value as
  on its alignment-task row. The record's ID is the call truvari kept for the cluster, often a
  parent's: it names the row only when the VCF has no OID. **mother_name** and **father_name** hold
  the parents' own call IDs the same way.
- Once the case has a slivar VCF, the staging view leaves out the proband's alignment task: the
  proband gets its DRAGEN set only when no slivar VCF is available. A DRAGEN set already loaded stays.
- A slivar VCF that cannot be downloaded fails **import_cnv_vcf** (a DRAGEN VCF is skipped with a
  warning): skipped, the proband would have no CNV set at all, and its staging row would be marked
  ingested with nothing loaded. A deleted task is never fetched: nothing of it reaches StarRocks, and
  its VCF may be gone from S3.
- The copy step of **insert_germline_cnv_occurrences** keeps the rows of the tasks not in the
  run, keyed on **task_id**, so a run with one set keeps the other. The cytoband, gene, SNV-count
  and gnomAD joins are keyed on **task_id** too.
- Keyed on **task_id**, the copy step also keeps the rows of an older alignment task of the same
  sequencing: a re-alignment adds a DRAGEN set, it no longer replaces the old one. The old set goes
  when its task is deleted in the clinical model. The portal's task selector opens the newest task.
- The CNV Exomiser report of the CNV annotation task (RAD-181) has its own path, apart from the SNV
  one. **load_exomiser_cnv_files** loads its contributing rows into **raw_exomiser_cnv**, tagged with
  the proband's sequencing and the task. Then, per tenant, **insert_exomiser_cnv** matches each row
  to its occurrence (the report's VCF_ID on **name**, else on the record ID) and ranks the rows of
  each occurrence into **exomiser_cnv**. **insert_germline_cnv_occurrences** copies the best row
  (**variant_rank** 1) into six columns, **exomiser_rank** to **exomiser_acmg_evidence**. The DRAGEN
  set keeps them empty.

See radiant-hub docs/features/RAD-169-cnv-post-processing.md.

---

## Gene coverage

Loads the DRAGEN coverage_by_gene CSV (**covgene** document) of each alignment into the tenant's
**gene_coverage** table: one row per CDS gene (RAD-315). It runs in Phase 3, like Exomiser.

| Task | Job |
|:--|:--|
| **extract_seq_ids** | The sequencing experiments of the part's delta, all tenants, deleted tasks included |
| **fetch_gene_coverage_files** | For each experiment, the newest alignment of each type in **current_alignment** and its **covgene** file |
| **load_gene_coverage_files** | After the Exomiser loads: one broker load per file into the shared **raw_gene_coverage**, tagged with the tenant |
| **insert_gene_coverage** | After **insert_exomiser**, per tenant: the CDS rows, with the gene name rule |
| **check_gene_coverage_files** | A leaf: fails the run with the S3 path and the error of each file that did not load |

- The alignments are **alignment_germline_variant_calling** and **alignment_somatic_variant_calling**
  (tumor-only). In a tumor-normal case only the normal has one: the tumor has no rows.
- The file is found from the experiment, not from the staging rows: the proband of a case with a
  slivar VCF keeps its coverage. An alignment shared by two cases is loaded once.
- Two formats, told apart by the header: the new one has a **type** column, and only its **CDS**
  rows are kept; in the old one every row is CDS. Any other header is a bad file.
- Gene name rule (QLIN's): a **\_chr** suffix (1 to 22, X, Y, M, MT: DRAGEN adds it to a name on more
  than one contig, e.g. the PAR genes) gives the symbol without it and the chromosome of the suffix;
  an **ENSG** name gives the **ensembl_gene** name of that id, else the id; a plain symbol stays, with
  its **ensembl_gene** chromosome (the first by chromosome when the name is on more than one).
  **gene** keeps the raw name. The mitochondrial chromosome is **M**, as in the variant tables
  (**ensembl_gene** says **MT**).
- The copy steps are keyed on the experiment, not the task: the run replaces the coverage of every
  experiment of the delta, so a re-alignment replaces the old alignment's coverage.
- An alignment without **covgene** gets no row, and the run stays green.
- A bad file (missing or unreadable on S3, unknown header, load cancelled or timed out) does not stop
  the others. The part gets the rows of every file that loaded, and the bad file's experiment keeps
  the rows an earlier import loaded (none if it never loaded). The experiment is marked imported all
  the same, so its file loads again only when the experiment is re-submitted by hand.
- StarRocks reads the header (a **FILES()** query) and the file (a broker load) with the same S3
  credentials: the Airflow worker never reads the bucket.

> A bad file never fails the load: the load returns its errors, and only **check_gene_coverage_files**
> fails, which no task waits on. So a bad file blocks no other data, and still fails the run. Any other
> error of the load (StarRocks down, lost connection, missing table) fails it before the swap, so the
> task retries and the loaded coverage stays; as for Exomiser, it stops the chain.

See radiant-hub docs/features/RAD-315-gene-coverage-germline-somatic-alignments.md.

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
