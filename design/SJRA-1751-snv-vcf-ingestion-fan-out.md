# SNV VCF ingestion — fan out somatic, single-writer the shared Iceberg tables

**Status:** implemented
**Related:** `SJRA-1751-somatic-snv-tumor-only-ingestion.md` (volumes), `SJRA-341-somatic-snv-ingestion.md`
(the shared-table decision), `SJRA-1187-pipeline-incremental-loading.md` (why a silent skip is
permanent), SJRA-1642 (the download-tolerance this removes), SJRA-1756 (the resource profiles)

## 1. Why

Somatic SNV extraction was a **single unmapped container** that walked every somatic task in the part
in sequence and committed all three Iceberg tables from inside that same pod. Germline, sitting in its
own sub-DAG, already did the opposite: one mapped writer per annotation task, fanning into one commit
pod.

That asymmetry was affordable while "somatic" meant tumor-normal, because TN VCFs are small. Tumor-only
removes the premise. From §4.1 of `SJRA-1751-somatic-snv-tumor-only-ingestion.md`, one TO WES sample
(39 MB gzipped, DRAGEN) yields:

| stage | rows |
| --- | --- |
| VCF records | 474,589 |
| on supported chromosomes | 470,485 |
| → `somatic_snv_occurrence` | 273,449 |
| → `snv_consequence` (**shared**) | 1,361,615 (mean 2.9 CSQ/record, max 87) |

A TO VCF is, in the design doc's words, "the patient's germline exome plus a small somatic tail" — so it
is germline-sized. And a part holds up to 100 (WGS) or 1000 (WXS) experiments, every one of them
processed serially in that one container.

## 2. The three defects

### 2.1 No fan-out, and all-or-nothing

Wall-clock was the **sum** over tasks rather than the max. Worse, `commit_partitions` ran only after the
last VCF, so any non-download exception on task N discarded the parquet files already written for tasks
1..N-1 — orphaned in object storage with no metadata referencing them. There was also no per-task
retry, and the pod took no `import_vcf` pool slot, so somatic's node pressure was invisible to the pool
that bounds germline's.

### 2.2 A real race on the shared tables, guarded by a retry that could not work

`snv_variant` and `snv_consequence` are written by **both** flows — deliberately, per §
SJRA-341, which renamed them from `germline_snv_*` "to reflect their shared ownership". Germline's
commit pod and somatic's inline commit were **siblings in `vcf_imports`**, so they ran concurrently by
construction.

Iceberg's optimistic concurrency prevented corruption, but the retry loop in `commit_files` could not
absorb a conflict:

```python
table.refresh()          # once, BEFORE staging
tx = table.transaction()
for partition in partition_to_commit:
    tx.delete(...); tx.add_files(...)     # captures AssertRefSnapshotId here

while max_retries > 0:
    try:
        tx.commit_transaction()           # same stale assertion, every attempt
```

Staging captures an `AssertRefSnapshotId` against the snapshot current at that moment. Re-invoking
`commit_transaction()` on the same transaction resends that stale assertion, so all 20 attempts failed
identically — burning ~20 s and then failing the part. (The log even said "Failed after 10 retries"
while `max_retries = 20`.)

The conflict was a **false** conflict: the two writers touch disjoint `task_id` partitions of an
`Identity(task_id)` spec, so the delete is a metadata-only whole-partition drop and a properly re-staged
retry always converges.

### 2.3 Deleted somatic tasks were re-ingested

`import_part.py` filtered `not t["deleted"]` into the config it handed germline, but passed somatic the
**raw** task list; `import_somatic_snv` filtered only on `task_type`.

## 3. What was built

One SNV DAG: two per-task fan-outs, one fan-in, one committer.

```
radiant-import-snv-vcf                      (was radiant-import-germline-snv-vcf)
  get_iceberg_namespace
  get_germline_tasks ─> create_germline_parquet_files_k8s.expand ─┐  pool=import_vcf, SNV profile
  get_somatic_tasks  ─> create_somatic_parquet_files_k8s.expand  ─┴─> merge_commits ─> commit_partitions_k8s
                                                                                       METADATA profile
radiant-import-part
  vcf_imports = [ import_snv_vcf (TriggerDagRun), import_cnv_vcf ]
```

**The race is now structural, not retried away.** `commit_partitions_k8s` is unmapped and is the only
task in the platform that commits `snv_variant` / `snv_consequence` for a part; the `import_part` pool
has one slot, so parts do not overlap either. One part is therefore exactly one snapshot per table.

Defect 2.3 falls out for free: the sub-DAG is fed by `prepare_config`, so somatic now inherits the
`deleted` filter.

Changes, by file:

| File | Change |
| --- | --- |
| `radiant/tasks/iceberg/utils.py` | `commit_files` re-stages the transaction from a fresh `table.refresh()` on **every** attempt, with exponential backoff + jitter; `_partition_filter_expr` extracted. Gained the shared `commit_partitions` (was duplicated in both `process.py` modules) and `merge_partition_commits`. |
| `radiant/tasks/vcf/snv/somatic/process.py` | `import_somatic_snv` / `merge_partitions_in_place` → `create_parquet_files(task, namespace)`, mirroring germline. |
| `radiant/dags/import_snv_vcf.py` | Was `import_germline_snv_vcf.py`. Two `get_*_tasks` + two fan-outs + one `merge_commits` + one commit. |

**One non-obvious detail:** `merge_commits` needs `trigger_rule=NONE_FAILED`. A part with no somatic
tasks — today's common case — expands that writer to **zero** mapped instances, which Airflow marks
SKIPPED; under the default `ALL_SUCCESS` the skip cascades and *nothing* commits, germline included. A
genuine extraction failure still blocks the commit as `upstream_failed`. This is the same reason
`sanity_check_delta_somatic_snv` and the `import_part` checkpoints already carry `NONE_FAILED`, and it is
pinned by `test_merge_commits_survives_a_flow_with_no_tasks`.

| `radiant/dags/operators/{k8s,ecs}.py` | `ImportGermlineSNVVCF` → `ImportSNVVCF`, split into `get_create_{germline,somatic}_parquet_files`; `ImportPart.get_import_somatic_snv_vcf` deleted. |
| `radiant/dags/import_part.py` | `import_somatic_snv_vcf` dropped from `vcf_imports`; the trigger now targets `-import-snv-vcf`. |
| `scripts/ecs/import_somatic_snv_vcf_for_task.py` | Takes `--task '<json>'` (one task) instead of `--tasks '<s3 path>'` (all of them). |

The retry fix stays as defence-in-depth for manual reruns and any future writer, and it is what the new
`tests/unit/iceberg/test_commit_files.py` pins: a re-staged transaction per attempt, one `refresh()` each.

## 4. Behaviour changes

**A failed somatic VCF download is now fatal.** SJRA-1642 was narrowly about `download_s3_file`
returning `None` and surfacing as a confusing cyvcf2 error; the remediation it asked for was to let the
exception propagate. The skip-one-and-continue policy that shipped alongside it was applied only to
somatic and CNV — the two single-container batch loops — and pointedly **not** to germline. It was a
mitigation for batching, and per-task fan-out removes the thing it mitigated.

Keeping it would have been actively harmful. A skipped task still *succeeds*, so `checkpoint_variants`
passes and `update_sequencing_experiments` marks the experiment ingested — and per SJRA-1187 the
incremental delta then never offers it again. A missing sample would be silently permanent. Failing
costs one map index, which can be cleared and retried on its own.

**CNV still has both gaps** (raw task list, download tolerance) — out of scope here, tracked separately.

**Somatic and germline are now one failure domain for the commit.** `merge_commits` keeps its default
`all_success`, so a somatic extraction failure blocks the germline commit for that part and vice versa.
That was already germline's own behaviour, and the flows were already coupled through the shared tables
and `checkpoint_after_vcf_imports`.

**Partial failure gets strictly better.** Before, one bad VCF discarded the whole part's parquet output.

**DAG rename.** `radiant-import-germline-snv-vcf` no longer exists; remove it from the Airflow DB/UI
after deploy. It is `schedule=None` and triggered with `reset_dag_run=True`, so no schedule history is
lost.

## 5. Considered and rejected

- **Separate somatic DAG mirroring germline, plus a 1-slot `iceberg_commit` pool.** Keeps independent
  failure domains and lets somatic be rerun alone. Rejected because it serializes the two committers
  anyway (so it buys no concurrency over the unified DAG) while adding a pool that must be created in
  docker-compose, MWAA and the k8s deploy, and leaving two near-identical DAGs to maintain.
- **Fan out inline in `import_part.py`.** Fewest files touched, but grows an already large DAG and
  leaves the race to be solved by a pool or by the retry alone.
- **Intra-VCF (per-chromosome) splitting.** Unnecessary: a TO VCF is single-sample, so per-pod it is
  *lighter* than the germline trio WGS the 4Gi/6Gi SNV profile was sized for. Per-task fan-out is enough.
- **Re-sizing the SNV/METADATA profiles.** Deliberately unchanged; revisit if TO WGS lands.

## 6. Still open

- `import_vcf` is 16 slots and is now shared by germline *and* somatic writers. Bounded node pressure is
  the point; raise it in the deployed environment rather than in code if TO throughput disappoints.
- The ECS path remains unsized per task (`_get_ecs_context` delegates entirely to
  `RADIANT_TASK_OPERATOR_TASK_DEFINITION`), so the SNV/CNV/METADATA profiles only apply on K8s.
- The somatic frequency rewrite of §8.1 of the tumor-only design (`is_tumor_normal` still grained on
  `case_id`, the four `pc/pn/pf_to_*` columns still hardcoded to zero) is untouched by this change.
