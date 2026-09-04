# Automating Nextflow quality control from cases — analysis

**Status:** implemented on `feat/sjra-1879` (2026-09-02). DAG `radiant/dags/nextflow_quality_control_cases.py`,
modules `radiant/tasks/nextflow/qc/`, query `sql/clinical/pending_quality_control_select.sql`,
runbook `radiant/dags/docs/nextflow_quality_control_cases.md`.

**Audience:** dev, QA, product owner.

**Related:** `SJRA-1698-nextflow-postprocessing-automation.md` and
`SJRA-1843-nextflow-postprocessing-from-cases.md` (the automation this mirrors — read 1698 §3, §5,
§7 first), `radiant/dags/docs/nextflow_quality_control.md` (the QC launcher runbook, in particular
the samplesheet and the "`dragen_metrics_dir` fails open" section).

**Related ticket:** SJRA-1879.

---

## 1. Why

`radiant-nextflow-quality-control` runs the Ferlab quality-control-pipeline in DRAGEN-metrics
mode, but only when someone hands it a samplesheet path and a `dragen_metrics_dir`. Nothing
decides *which* samples need a report and nothing writes the report back onto the case.

For post-processing that loop is already closed: `radiant-nextflow-postprocessing-cases` asks the
clinical model which cases are pending, builds the inputs, triggers the launcher DAG, collects
what it published and registers it on the cases through the portal batch PATCH. We want the same
for QC, registering on each case a task of type `quality_control_metrics` ("Quality Control
Metrics") whose output document has data type `aggqc` ("Aggregate Quality Control Report").

The question this document answers: extend `radiant-nextflow-postprocessing-cases`, or add a
second automation DAG.

## 2. Decision: a new DAG, sharing infrastructure with the existing one

A new scheduled DAG — working name `radiant-nextflow-quality-control-cases` — that triggers the
existing `radiant-nextflow-quality-control` launcher. QC is **not** folded into
`radiant-nextflow-postprocessing-cases`.

### 2.1 What the two automations share, and what they do not

| | Post-processing cases | QC cases |
|---|---|---|
| Eligibility signal | (case, experiment) has a gVCF and no `radiant_germline_annotation` | (case, experiment) has an alignment (CRAM + DRAGEN metrics) and no `quality_control_metrics` |
| Unit of work | family / case: joint calling, PED, phenopackets | participant / sample rows; `familyId` optional |
| Samplesheet | `familyId,sample,sequencingType,gvcf,familyPheno,familyPed` | `participant,sample,fileType,file1,file2,familyId,experimentalStrategy,sex,…` |
| Extra inputs | HPO phenotypes | `dragen_metrics_dir` — no analogue in post-processing |
| Registered | slivar VCF + exomiser tsv/html/json, two task types | one `aggqc` document, one task type |
| Launcher DAG | `radiant-nextflow-postprocessing` | `radiant-nextflow-quality-control` |

They share the *shape* — discover, build inputs, trigger, collect, register — and almost none
of the *content*.

### 2.2 Why not the same DAG

- **It would split the single eligibility statement.** 1698 §7 made `pending_annotation_select.sql`
  one statement on purpose: eligibility and membership are defined once so a superseded
  experiment cannot keep its case eligible for ever. A second anti-join on a different task type,
  with a different document as the trigger, reintroduces the drift that decision removed.
- **It couples scheduling and failure.** `max_active_runs=1` is the whole concurrency story of the
  cases DAG. A QC run would queue behind a multi-hour annotation run, or the reverse, and a QC
  failure would either block annotation registration or need branch-specific trigger rules.
- **One run, two unrelated registrations.** A partial failure leaves a case annotated but not
  QC'd, with no clean unit to retry.
- **The evidence already points this way.** The QC launcher had to isolate itself from
  post-processing twice — the `qc-` RUN_TAG prefix and the separate ConfigMap pair. Both design
  docs, when an adjacent need appeared (1843 §10, 1698 §11), chose "a separate, narrower job".
- **The existing DAG is germline-annotation-shaped end to end** — `case_type_code = 'germline'`,
  one proband and one strategy per family, gVCF only. QC is germline-only for now (§4) but is
  naturally broader, and a new DAG can widen its scope later without un-scoping post-processing.

## 3. What is reused, what is new

### 3.1 Reused as-is

- `radiant/tasks/nextflow/paths.py` — run-tag → S3 / pod path derivation. Its only
  pipeline-specific detail is the `samplesheet.csv` filename, which QC also uses.
- `radiant/tasks/nextflow/portal.py` — token, `PATCH /{tenant}/cases/batch`, batch polling.
- The DAG skeleton of `radiant/dags/nextflow_postprocessing_cases.py`: StarRocks discovery with
  `rows_output_processor`, the strict/lenient `select_cases` split (named `task_ids` are an
  operator's request, discovery is the job's own doing), `AirflowSkipException` on an empty scope,
  the S3 input prefix cleared then written behind the own-prefix guard, `TriggerDagRunOperator`
  with a pinned `trigger_run_id` so a retry resumes rather than re-runs, `collect_outputs` polling
  for the FSx export lag, per-tenant mapped `register_tasks` (PATCH appends, so a looping task
  that failed halfway would double-register), `max_active_runs=1`, `@daily`.
- The SQL pattern of `pending_annotation_select.sql`: newest `completed` experiment per (case,
  member), newest alignment per experiment, `exclusion_reason` emitted by the query, tenants as a
  grant list not a filter, `task_ids` narrowing candidacy never membership.

### 3.2 New

- `radiant/dags/sql/clinical/pending_quality_control_select.sql`. Same CTE structure, but the
  documents it returns are **all** `output` documents of the current alignment task (the CRAM and
  crai for the samplesheet, every url for the metrics-directory candidates of §4.1) and the
  anti-join is on `quality_control_metrics`. The `current_experiment` and
  `current_alignment` CTEs are the same logic as the annotation query's — extract them into a
  Jinja include consumed by both templates, so the supersession policy of 1698 §5 stays defined
  once even across two DAGs.
- A QC input builder — samplesheet only, no PED, no phenopackets — and a QC output spec and
  batch-body builder for `quality_control_metrics` → `aggqc`. A sibling package
  (`radiant/tasks/nextflow_qc/` or `radiant/tasks/nextflow/qc/`) importing `paths` and `portal`
  from the existing one; `model`, `resolve`, `inputs`, `outputs`, `batch` stay untouched.
- The launcher DAG `nextflow_quality_control.py` does not change. The new DAG passes `input`,
  `dragen_metrics_dir` and `outdir` through `conf`, exactly as post-processing-cases does.
- `quality_control_metrics` and `aggqc` added to the test fixture
  `tests/resources/clinical/create_clinical_tables.sql` and to `radiant/dags/sql/clinical/seeds.sql`
  (one case with an alignment and no QC task, one already QC'd). The authoritative value sets are
  portal-side and already contain both codes in QA; the fixtures only mirror them.

## 4. Decisions taken

1. **DRAGEN metrics location: found next to the alignment's documents, one directory per
   launcher run.** The metrics are not documents in the clinical model. In the layouts seen so
   far they sit in the same directory as *some* output of the alignment task — the gVCF in one
   setup, the CRAM in another — so the directory is derived from **all** output documents of the
   member's current alignment task (`cram`, `crai`, `gvcf`, `tbi`, `gcnv` VCF, …), not from the
   CRAM alone: the distinct parent directories of those urls are the candidate directories.

   Rather than guess which candidate holds the metrics, **probe**: `generate_inputs` already has
   S3 access, so it lists each candidate for `<aliquot>.<anything>.mapping_metrics.csv` -- sample
   before the first dot, type as the suffix, the same rule the pipeline applies, so
   `.final.` and `.dragen.` infixes both count -- and keeps the one that answers. A member with no hit is
   excluded with a reason (`no_dragen_metrics`), like `no_gvcf` today. Probing also checks the
   naming contract below before a pod is spent on it. The metrics must live in the FSx-imported
   bucket, since the launcher gets a pod path via `paths.to_mount`.

   `--dragen_metrics_dir` is a **single directory per launcher run**. Verified against
   quality-control-pipeline v2.0.0 (the revision pinned in `Dockerfile.nextflow.launcher`):

   - The value is interpolated straight into globs — `"${params.dragen_metrics_dir}/*.csv"` and
     `"${params.dragen_metrics_dir}/**/*.csv"` in `workflows/qualitycontrol.nf`. A
     **comma-separated list is not split**: it becomes a literal path that matches nothing, and
     because the channel is `checkIfExists: false` the run goes green with an empty report.
   - Nextflow's `fromPath` does accept a brace set under a common prefix
     (`/root/{runA,runB}` matched both in a local smoke test; a leading brace over absolute paths
     matched nothing). But the param is declared `format: directory-path` in
     `nextflow_schema.json`, and nf-schema 2.3.0's `FormatDirectoryPathEvaluator` rejects any
     value that `file()` expands to a list: *"is not a directory, but a file path pattern"*. So
     brace expansion fails at launch unless validation of that param is switched off in the
     config — not a road to take.
   - Files are matched to samplesheet rows by **exact first dot-token** of the filename
     (`f.name.tokenize('.')[0]` combined with the `sample` column), not by prefix. DRAGEN names
     its files after the **aliquot**, so the samplesheet `sample` column is
     `sequencing_experiment.aliquot` (already returned by the discovery query), and a directory that is too
     wide is dangerous exactly as suspected: the same sample name under two subdirectories (a
     re-alignment, another batch) attaches **both** files to that sample, and the per-gene
     coverage `join` on `sample|region` then pairs them unpredictably.

   Rule, therefore: **group the pending cases by resolved metrics directory and fire one
   launcher run per group.** In practice a night's pending cases span many directories — each
   sequencing run or case lands in its own — so the grouping rarely merges cases and the normal
   shape is **one launcher run per group of neighbouring directories**, up to five side by side on
   the launcher (`max_active_runs=5`). That
   is acceptable at nightly volumes because DRAGEN-metrics mode is light (somalier + MultiQC, no
   BAM or VCF recompute), but it makes the upstream change worth requesting: teach the pipeline to
   accept a list of directories (or a per-row `dragenMetricsDir` samplesheet column), after which
   the grouping collapses to one run a night with no change to this DAG beyond the input builder.
   A case whose members resolve to different directories does not fit one run today: use their
   common ancestor only if listing it shows no duplicate `<aliquot>.` prefixes for the run's
   samples; otherwise exclude the case with a reason (`metrics_dir_split`).

2. **Granularity: one `quality_control_metrics` task per case, carrying the whole per-family
   MultiQC output.** With `cohort_mode = false` (the pipeline default and what `nextflow-qc-params`
   runs) MultiQC runs **once per `familyId`**. Confirmed on a real QA run (S3 listing of
   `…/qc/multiqc/1KGP-1463/`, 2026-08-28), a family publishes:

   | File | Register as | Notes |
   |---|---|---|
   | `multiqc/{familyId}/{familyId}_multiqc_report.html` | `aggqc` / `html` | the report itself — **mandatory** |
   | `multiqc/{familyId}/{familyId}_multiqc_report_data.zip` | `aggqc` / `zip` | MultiQC parsed data — **mandatory** |
   | `multiqc/{familyId}/qc_json/{aliquot}.metrics.json` | **not registered** | per-sample GA4GH sidecar; a strict subset of the archive's tables (decision 2026-09-03, after inspecting a real archive) |

   Sizes were real (3.7 MB html, 1.6 MB zip), so the symlink-publish trap of the runbook did not
   occur in QA.

   With `familyId = CA<case id>` a launcher run covering several cases yields one such set per
   case, so no per-case launcher runs are needed for the report to be per case. The task binds to
   all of the case's current experiments — multi-aliquot, like the annotation task
   (`task_context.sequencing_experiment_id` is NOT NULL, so a task can never be case-only).
3. **Independent of post-processing.** Separate nightly DAG, separate eligibility. A case may be
   QC'd and annotated in either order; QC does not gate annotation.
4. **Samplesheet `sample` = aliquot.** DRAGEN names its metric files `<aliquot>.<type>.csv`, and
   the pipeline matches on the exact first dot-token, so the `sample` column carries
   `sequencing_experiment.aliquot`. `participant` carries the patient id. This is also the probe
   key of §4.1.
5. **Portal side is ready.** `quality_control_metrics` and `aggqc` exist in the QA portal's value
   sets. The task declares the CRAM and crai as `input_documents`; they already exist in the tenant,
   so TASK-005 holds whether or not the portal enforces TASK-003 for this type.
6. **Germline only.** `case_type_code = 'germline'`, `alignment_germline_variant_calling` tasks,
   mirroring post-processing. The QC samplesheet supports tumor rows (`status = 1`), so somatic is
   a later widening of this DAG, not a change to post-processing.

## 5. The DAG, in one picture

```
discover_scope -> select_cases -> group_by_metrics_dir -> generate_inputs (mapped, one per group)
   (StarRocks)      (PyOp)             (PyOp)                        |
                                                             run_pipeline (mapped TriggerDagRun,
                                                                          one launcher run per group)
                                                                          |
              register_tasks (per tenant) <- collect_outputs <------------+
```

The mapping unit is the resolved metrics directory, not the case: one samplesheet, one input prefix and
one launcher run per group, with the child `trigger_run_id` pinned per group so a retry resumes
that group's run. `collect_outputs` and `register_tasks` gather across groups; registration is
still one batch PATCH per tenant.

No `fetch_phenotypes` / `resolve_cases`: the QC samplesheet needs sex, relationship and affected
status, which the discovery rows already carry.

## 6. Out of scope

- Registering DRAGEN metrics as documents. They stay filesystem inputs located by probing next to the alignment's documents (§4.1).
  If the convention proves unreliable this is the fallback, and it needs portal-side data types.
- Full recompute mode (`BAM_QC` / `VCF_QC`). The launcher only runs DRAGEN-metrics mode.
- The upstream pipeline change (a list of metrics directories). Worth a ticket on
  quality-control-pipeline; this DAG works without it, one run per case.
- Somatic cases, and any change to `radiant-nextflow-postprocessing-cases`.

## 7. How this gets verified

- Unit: render `pending_quality_control_select.sql` (mirror `tests/unit/dags/test_clinical_case_sql_render.py`);
  DAG structure tests mirroring `tests/unit/dags/test_nextflow_postprocessing_cases.py`;
  input / output / batch builders mirroring `tests/unit/nextflow/`.
- Integration: `make test-integration` with the extended seeds — the un-QC'd case is discovered,
  the QC'd one is not, a member without an alignment comes back with a reason.
- QA: trigger with `task_ids` for one case and `dry_run=true`; check the samplesheet and the
  derived metrics dir on S3; then a real run and confirm the `aggqc` document lands on the case.

## 8. Open items

- **A `zip` file format code portal-side.** Being added to QA; the test fixture already has it.
  Until it lands, a real registration of `{familyId}_multiqc_report_data.zip` fails validation.

Everything else is settled: the report layout was confirmed on a real QA run, the task type and
`aggqc` data type exist in QA, and the remaining facts come from the pipeline source at the pinned
revision. Implementation can start from §3 and §5.

## 9. Implementation notes (what differs from §3/§5 as drafted)

- The annotation query was left untouched, and the two `current_*` CTEs are duplicated rather than
  shared through a Jinja include: the SQL render tests load each template standalone. A unit test
  (`test_the_supersession_ctes_are_identical_to_the_annotation_query`) pins the two copies equal.
- The query returns **one row per alignment output document** rather than one per member, and
  `qc.resolve.fold_rows` folds them. StarRocks' `GROUP_CONCAT` would have done it in SQL, but its
  `DISTINCT ... ORDER BY` form is version-dependent over the JDBC catalog.
- Grouping is its own task (`group_cases`): Airflow can only map over a task's return value, not a
  keyed XCom, so `locate_metrics` returns the kept cases and `group_cases` the list to expand.
- `register_case_batch` was extracted to `radiant/tasks/nextflow/register.py`; the post-processing
  DAG still carries its own copy and can be switched over separately.
- First QA run (2026-09-03) found two defects, both fixed: grouping only merged directories
  *within* a case, so a night with one sample per subfolder produced one run per case instead of
  one run at their common ancestor; and the pinned child run id inherited the parent's
  `scheduled__` prefix, which Airflow 3.2.1 refuses for an operator-triggered run
  (`create_dagrun` → `DagRunType.from_run_id`), surfacing as a 500. `sanitize_run_tag` now drops
  the `__`, which also fixes the same latent defect in the post-processing cases DAG.
- The QA layout that motivated probing over convention, seen on a real case (`prag/GM232700/`): the
  CRAM sits under `Mapper/v4.0.3-hg38_…/`, while the gVCF **and** every DRAGEN metrics CSV sit under
  `vcf/dragen/v1.2.2_dragen4.0.3-hg38_…/varcaller/`, with a `.dragen.` infix in every filename.
  Pipeline-side observations from that run, not DAG defects: DRAGEN 4.0.3 wrote
  `wgs_overall_mean_cov.csv` / `wgs_hist.csv` / `wgs_contig_mean_cov.csv` but no
  `wgs_coverage_metrics.csv`, so the pipeline's report leaves Coverage `na` although MultiQC's built-in
  DRAGEN module reads the mean coverage (52.56×); and the `.dragen` infix is not in the MultiQC
  `extra_fn_clean_exts`, so built-in sections label samples `GM232700.dragen`. Both belong upstream.
- The seeds gain task 71, a `quality_control_metrics` over case 1, so case 1 is the seeded
  "already QC'd" case and case 16 the seeded pending trio.

## Annex A. Worked example: what one case registers

Illustrative trio, tenant `radiant`, case `1` (`submitter_case_id = 'CA-0001'`, project `P01`),
members with current experiments `101` / `102` / `103` and aliquots `NA12878` / `NA12891` /
`NA12892`. The run tag is `qc-cases-2026-09-02` and `NEXTFLOW_OUTPUTS_ROOT` is
`s3://qlin-qa-nextflow-outputs-…/`. The samplesheet gave every member `familyId = CA1`.

### A.1 The batch PATCH body (`PATCH /radiant/cases/batch`)

```json
{
  "cases": [
    {
      "project_code": "P01",
      "submitter_case_id": "CA-0001",
      "tasks": [
        {
          "type_code": "quality_control_metrics",
          "aliquots": ["NA12878", "NA12891", "NA12892"],
          "pipeline_name": "quality-control-pipeline",
          "pipeline_version": "2.0.0",
          "genome_build": "GRch38",
          "input_documents": [
            {"url": "s3://…/dragen/run-42/NA12878.cram"},
            {"url": "s3://…/dragen/run-42/NA12878.cram.crai"},
            {"url": "s3://…/dragen/run-42/NA12891.cram"},
            {"url": "s3://…/dragen/run-42/NA12891.cram.crai"},
            {"url": "s3://…/dragen/run-42/NA12892.cram"},
            {"url": "s3://…/dragen/run-42/NA12892.cram.crai"}
          ],
          "output_documents": [
            {"name": "CA1_multiqc_report.html",     "url": "s3://…/qc-cases-2026-09-02/multiqc/CA1/CA1_multiqc_report.html",     "size": 3772915, "data_category_code": "genomic", "data_type_code": "aggqc", "format_code": "html"},
            {"name": "CA1_multiqc_report_data.zip", "url": "s3://…/qc-cases-2026-09-02/multiqc/CA1/CA1_multiqc_report_data.zip", "size": 1616232, "data_category_code": "genomic", "data_type_code": "aggqc", "format_code": "zip"}
          ]
        }
      ]
    }
  ]
}
```

`aliquots` is how the portal resolves the task to sequencing experiments (and so to
`task_context` rows). `input_documents` name documents that already exist in the tenant (the
alignment task's outputs), which is what TASK-005 requires. Sizes come from the S3 listing, like
today's collector; `hash` is not computed.

### A.2 The rows the portal writes

`task` — one row per case:

| id | task_type_code | pipeline_name | pipeline_version | genome_build | created_on | tenant_code |
|---|---|---|---|---|---|---|
| 1001 | `quality_control_metrics` | `quality-control-pipeline` | `2.0.0` | `GRch38` | 2026-09-02 … | `radiant` |

`task_context` — one row per (case, experiment). Unlike alignment tasks, `case_id` is set, which
is what lets `pending_quality_control_select.sql` anti-join on the (case, seq) pair:

| task_id | case_id | sequencing_experiment_id |
|---|---|---|
| 1001 | 1 | 101 |
| 1001 | 1 | 102 |
| 1001 | 1 | 103 |

`document` — one row per output file (new ids; the input CRAM/crai rows already exist from the
alignment registration):

| id | name | data_category_code | data_type_code | format_code | size | url | tenant_code |
|---|---|---|---|---|---|---|---|
| 2001 | `CA1_multiqc_report.html` | `genomic` | `aggqc` | `html` | 3772915 | `s3://…/multiqc/CA1/CA1_multiqc_report.html` | `radiant` |
| 2002 | `CA1_multiqc_report_data.zip` | `genomic` | `aggqc` | `zip` | 1616232 | `s3://…/multiqc/CA1/CA1_multiqc_report_data.zip` | `radiant` |

`task_has_document` — outputs point at the two new rows, inputs at the alignment task's existing
CRAM/crai documents (ids `63`/`64`-style rows in the seeds):

| task_id | document_id | type |
|---|---|---|
| 1001 | 2001 | `output` |
| 1001 | 2002 | `output` |
| 1001 | *(NA12878 cram id)* | `input` |
| 1001 | *(NA12878 crai id)* | `input` |
| 1001 | *(NA12891 cram id)* | `input` |
| 1001 | … | `input` |

### A.3 What this means for eligibility and re-runs

- The next nightly `discover_scope` sees a `quality_control_metrics` task with `case_id = 1` for
  experiments 101–103 and stops proposing case 1. A member re-sequenced later gets a new
  experiment id, so the pair `(1, new seq)` is pending again and only that case is redone.
- PATCH appends. A deliberate re-run adds a second `quality_control_metrics` task (id 1002) with
  its own documents alongside the first, exactly as annotation re-runs do; nothing is replaced.
- The `staging_external_sequencing_experiment` view whitelists (task type, data type/format)
  pairs, so `aggqc` documents are invisible to the variant ETL — the intended outcome.
