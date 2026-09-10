# Nextflow Quality Control (from Cases)

Finds the germline cases that have been aligned but never quality-controlled, runs the Ferlab
[quality-control-pipeline](https://github.com/Ferlab-Ste-Justine/quality-control-pipeline)
over them in DRAGEN-metrics mode, and registers the per-family MultiQC report back onto each
case as a `quality_control_metrics` task.

`radiant-nextflow-quality-control` runs the pipeline and nothing else. This DAG closes
everything around it, exactly as `radiant-nextflow-postprocessing-cases` does for annotation.

```
discover_scope -> select_cases -> locate_metrics -> group_cases -> generate_inputs (one per dir)
                                                                          |
                                                                    run_pipeline (one per dir)
                                                                          |
          register_tasks (one per tenant) <- collect_outputs <------------+
```

It runs **daily and takes no input**. The parameters exist for targeted reruns and for
configuration, not for normal operation.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `task_ids` | empty | `alignment_germline_variant_calling` task ids, for a targeted rerun. Empty means "find everything". |
| `tenants` | `$NEXTFLOW_QC_TENANTS`, else `$NEXTFLOW_POSTPROCESSING_TENANTS` | Tenants the portal has granted this service account `ingest_data` on. Empty means no filtering. |
| `dry_run` | **false** | Passed to the batch PATCH. True validates and writes nothing. |

**`dry_run` on a scheduled run is a trap.** Nothing gets registered, so every case stays
eligible and tomorrow's run does the same work again. Manual runs only.

## What counts as "needs QC"

A `(case, sequencing experiment)` pair is eligible when the pair's *current* experiment has an
alignment publishing a CRAM, and no `quality_control_metrics` task is scoped to that same pair.
The rule lives in `sql/clinical/pending_quality_control_select.sql`, and its two selections
are the same as the annotation query's: **one experiment per (case, member)**, the newest
`completed` one, and **one alignment per experiment**, the newest. Keep the two templates'
`current_*` CTEs identical when touching either.

The query returns one row per **output document** of the alignment, not one per member. That is
how the metrics are found (below).

## Where the DRAGEN metrics come from

The metrics CSVs are **not documents** in the clinical model. They sit in the same directory
as *some* output of the alignment -- the gVCF in one layout, the CRAM in another -- so
`locate_metrics` takes the parent directory of every output document of the member's
alignment and **probes S3** for `<aliquot>.<anything>.mapping_metrics.csv` in each, the same rule the
pipeline uses (`NA12878.mapping_metrics.csv`, `NA12878.final.…`, `GM232700.dragen.…` all count). The one that answers is the member's metrics directory.

Probing rather than assuming a convention is deliberate. The pipeline matches metrics files to
samplesheet rows by the **exact first dot-token** of the filename and **fails open**: a wrong
directory produces a green run with a half-empty report. Hence also the samplesheet's `sample`
column is the **aliquot**, since that is what DRAGEN names its files after.

`--dragen_metrics_dir` is **one directory per Nextflow run** (a comma-separated list is not
split and matches nothing). Cases are therefore grouped by the directory the probe found, neighbouring
directories are merged into their common parent whenever that parent is still in the workspace
bucket, below the bucket root, and holds exactly one metrics file per aliquot, and **one launcher
run is fired per resulting group**, each with its own run tag, input prefix and outdir. Seven cases
under `individuals/` and one under `prag/` make two runs, not eight.
Since a night's cases usually span many directories, expect one child run per case, queued
up to five at a time on the launcher. A case whose members' metrics sit in different
directories may use their common ancestor, but only if that ancestor is still in the workspace
bucket and holds no duplicated sample; otherwise it is excluded.

## Nothing fails the run -- it gets excluded

A candidate that cannot be run is dropped with a reason and the rest continues. `select_cases`
and `locate_metrics` each log theirs; grep for `excluded`.

| Reason | Meaning | Transient? |
|---|---|---|
| `pending_sequencing` | a member has no `completed` sequencing experiment yet | yes |
| `pending_alignment` | a member's current experiment has no alignment task yet | yes |
| `no_cram` | the current alignment published no CRAM; somalier needs one | no |
| `ambiguous_cram` | more than one CRAM on one alignment task -- a mistyped document | no, fix the data |
| `proband_count` | two different patients are marked proband on the case | no, fix the data |
| `unsupported_strategy` | a strategy outside `{wgs, wxs, wes}` | no |
| `no_project_code` | `cases.project_id` did not resolve; the batch PATCH needs it | no |
| `tenant_not_granted` | the tenant is not in `tenants` | no, grant it |
| `no_dragen_metrics` | no `mapping_metrics.csv` for the aliquot in any candidate directory | until the metrics land |
| `ambiguous_dragen_metrics` | the aliquot's metrics are in two candidate directories | no, fix the data |
| `metrics_not_on_workspace` | the metrics are in a bucket the FSx mount does not import | no |
| `metrics_dir_split` | members' metrics in different directories with no safe common ancestor | no |

If every candidate is excluded the run **skips**. A skipped run is not a failed one.

**A targeted rerun behaves differently on purpose.** With `task_ids` set, an unresolvable case
-- at selection or at the probe -- fails the run instead of being excluded.

## What gets registered

One `quality_control_metrics` task per case, bound to every member's aliquot, with the
alignment's CRAM and index as input documents and, as outputs, everything the pipeline
published for the family under `multiqc/CA<case id>/`:

| File | data_type / format |
|---|---|
| `CA<id>_multiqc_report.html` | `aggqc` / `html` |
| `CA<id>_multiqc_report_data.zip` | `aggqc` / `zip` |

The pipeline also writes `qc_json/<aliquot>.metrics.json` per sample. They are not registered:
everything in them is in the archive's tables, so they only lengthened each case's document list.

`collect_outputs` requires the complete set for every case in a run; a partial run registers
nothing. PATCH **appends**: a deliberate re-run adds a second task alongside the first.

## Where things are written

| Var | Default | Used by |
|---|---|---|
| `NEXTFLOW_INPUTS_ROOT` | -- | `generate_inputs` writes here (an `s3://` uri); the metrics must also be under this bucket |
| `NEXTFLOW_OUTPUTS_ROOT` | -- | `collect_outputs` lists here (an `s3://` uri) |
| `NEXTFLOW_INPUTS_MOUNT` | `/workspace/inputs` | the pod path the inputs bucket appears at |
| `NEXTFLOW_OUTPUTS_MOUNT` | `/workspace/outputs` | the pod path the outputs bucket appears at |
| `NEXTFLOW_QC_TENANTS` | `$NEXTFLOW_POSTPROCESSING_TENANTS` | comma-separated default for the `tenants` param |

Each group -- each child run -- gets its own subdirectory, named after this run and the group
index:

```
{NEXTFLOW_INPUTS_ROOT}/qc-runs/{run_tag}-g{n}/samplesheet.csv
{NEXTFLOW_OUTPUTS_ROOT}/qc/{run_tag}-g{n}/multiqc/CA<id>/…
```

The `qc-runs/` and `qc/` subdirectories keep these apart from post-processing, which writes
`postprocessing-runs/` and `postprocessing/` under the same two roots.

The child run id is pinned to `{run_tag}-g{n}`, so a retry of this DAG re-enters the same
launcher run and its Nextflow launch directory, and `-resume` skips what already completed.
The launcher prefixes its own `qc-` to it, which keeps it apart from post-processing.

The tag also drops the `__` after the run type (`scheduled__…` becomes `scheduled-…`). Airflow
3.2 refuses an operator-triggered run whose id starts with `scheduled__`, since that prefix is
reserved for scheduled runs, and reports it as an opaque 500 from the API server.
