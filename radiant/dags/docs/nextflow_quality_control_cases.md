# Nextflow Quality Control (from Cases)

Finds the germline cases that have been aligned but never quality-controlled, runs the Ferlab
[quality-control-pipeline](https://github.com/Ferlab-Ste-Justine/quality-control-pipeline)
over them in DRAGEN-metrics mode, and registers the per-family MultiQC report back onto each
case as a **quality_control_metrics** task.

**radiant-nextflow-quality-control** runs the pipeline and nothing else. This DAG closes
everything around it, exactly as **radiant-nextflow-postprocessing-cases** does for
annotation.

| # | Task | Does |
|:--|:--|:--|
| 1 | **discover_scope** | Establishes what this run is allowed to look at |
| 2 | **select_cases** | Finds eligible cases, excludes the rest with a reason |
| 3 | **locate_metrics** | Probes S3 for each member's DRAGEN metrics directory |
| 4 | **group_cases** | Groups cases by the directory the probe found |
| 5 | **generate_inputs** | One mapped instance per group |
| 6 | **run_pipeline** | One mapped instance per group |
| 7 | **collect_outputs** | Lists what the pipeline published |
| 8 | **register_tasks** | PATCHes the portal, one mapped instance per tenant |

It runs **daily and takes no input**. The parameters exist for targeted reruns and for
configuration, not for normal operation.

---

## Parameters

| Param | Default | Meaning |
|:--|:--|:--|
| **task_ids** | empty | alignment_germline_variant_calling task ids, for a targeted rerun. Empty means "find everything" |
| **tenants** | $NEXTFLOW_QC_TENANTS, else $NEXTFLOW_POSTPROCESSING_TENANTS | Tenants the portal has granted this service account ingest_data on. Empty means no filtering |
| **dry_run** | **false** | Passed to the batch PATCH. True validates and writes nothing |

> **dry_run on a scheduled run is a trap.** Nothing gets registered, so every case stays
> eligible and tomorrow's run does the same work again. Manual runs only.

---

## What counts as "needs QC"

A (case, sequencing experiment) pair is eligible when the pair's *current* experiment has an
alignment publishing a CRAM, and no **quality_control_metrics** task is scoped to that same
pair. The rule lives in sql/clinical/pending_quality_control_select.sql, and its two
selections are the same as the annotation query's:

- **One experiment per (case, member)** — the newest completed one.
- **One alignment per experiment** — the newest.

> Keep the two templates' current CTEs identical when touching either.

The query returns one row per **output document** of the alignment, not one per member. That
is how the metrics are found, below.

---

## Where the DRAGEN metrics come from

The metrics CSVs are **not documents** in the clinical model. They sit in the same directory
as *some* output of the alignment — the gVCF in one layout, the CRAM in another. So
**locate_metrics** takes the parent directory of every output document of the member's
alignment and **probes S3** for a mapping_metrics.csv whose first dot-token is the aliquot,
the same rule the pipeline uses. NA12878.mapping_metrics.csv, NA12878.final and
GM232700.dragen all count. The directory that answers is the member's metrics directory.

Probing rather than assuming a convention is deliberate. The pipeline matches metrics files
to samplesheet rows by the **exact first dot-token** of the filename and **fails open**: a
wrong directory produces a green run with a half-empty report. That is also why the
samplesheet's **sample** column is the **aliquot** — that is what DRAGEN names its files
after.

### One directory per run, so cases are grouped

The pipeline's dragen_metrics_dir takes **one directory per Nextflow run**; a comma-separated
list is not split and matches nothing. So:

1. Cases are grouped by the directory the probe found.
2. Neighbouring directories are merged into their common parent — but only when that parent
   is still in the workspace bucket, below the bucket root, and holds exactly one metrics
   file per aliquot.
3. **One launcher run is fired per resulting group**, each with its own run tag, input prefix
   and outdir.

Seven cases under individuals/ and one under prag/ make two runs, not eight. Since a night's
cases usually span many directories, expect roughly one child run per case, queued up to five
at a time on the launcher.

A case whose members' metrics sit in different directories may use their common ancestor, but
only if that ancestor is still in the workspace bucket and holds no duplicated sample.
Otherwise it is excluded.

---

## Nothing fails the run — it gets excluded

A candidate that cannot be run is dropped with a reason and the rest continues.
**select_cases** and **locate_metrics** each log theirs; grep for "excluded".

| Reason | Meaning | Transient? |
|:--|:--|:--|
| **pending_sequencing** | A member has no completed sequencing experiment yet | Yes |
| **pending_alignment** | A member's current experiment has no alignment task yet | Yes |
| **no_cram** | The current alignment published no CRAM, and somalier needs one | No |
| **ambiguous_cram** | More than one CRAM on one alignment task — a mistyped document | No, fix the data |
| **proband_count** | Two different patients are marked proband on the case | No, fix the data |
| **unsupported_strategy** | A strategy outside {wgs, wxs, wes} | No |
| **no_project_code** | cases.project_id did not resolve, and the batch PATCH needs it | No |
| **tenant_not_granted** | The tenant is not in **tenants** | No, grant it |
| **no_dragen_metrics** | No mapping_metrics.csv for the aliquot in any candidate directory | Until the metrics land |
| **ambiguous_dragen_metrics** | The aliquot's metrics are in two candidate directories | No, fix the data |
| **metrics_not_on_workspace** | The metrics are in a bucket the FSx mount does not import | No |
| **metrics_dir_split** | Members' metrics in different directories with no safe common ancestor | No |

If every candidate is excluded the run **skips**. A skipped run is not a failed one.

> **A targeted rerun behaves differently on purpose.** With **task_ids** set, an unresolvable
> case — at selection or at the probe — fails the run instead of being excluded.

---

## What gets registered

One **quality_control_metrics** task per case, bound to every member's aliquot, with the
alignment's CRAM and index as input documents and, as outputs, everything the pipeline
published for the family under its multiqc/CA directory:

| File | data_type | format |
|:--|:--|:--|
| CA_ID_multiqc_report.html | aggqc | html |
| CA_ID_multiqc_report_data.zip | aggqc | zip |

The pipeline also writes a qc_json metrics.json per sample. Those are **not** registered:
everything in them is in the archive's tables, so they only lengthened each case's document
list.

**collect_outputs** requires the complete set for every case in a run; a partial run
registers nothing. PATCH **appends**, so a deliberate re-run adds a second task alongside the
first.

---

## Where things are written

| Var | Default | Used by |
|:--|:--|:--|
| **NEXTFLOW_INPUTS_ROOT** | — | **generate_inputs** writes here. An s3:// uri, and the metrics must also be under this bucket |
| **NEXTFLOW_OUTPUTS_ROOT** | — | **collect_outputs** lists here. An s3:// uri |
| **NEXTFLOW_INPUTS_MOUNT** | /workspace/inputs | The pod path the inputs bucket appears at |
| **NEXTFLOW_OUTPUTS_MOUNT** | /workspace/outputs | The pod path the outputs bucket appears at |
| **NEXTFLOW_QC_TENANTS** | $NEXTFLOW_POSTPROCESSING_TENANTS | Comma-separated default for the **tenants** param |

Each group — each child run — gets its own subdirectory, named after this run and the group
index:

- INPUTS_ROOT / qc-runs / RUN_TAG-gN / samplesheet.csv
- OUTPUTS_ROOT / qc / RUN_TAG-gN / multiqc / CA_ID /

The qc-runs/ and qc/ subdirectories keep these apart from post-processing, which writes
postprocessing-runs/ and postprocessing/ under the same two roots.

The child run id is pinned to RUN_TAG-gN, so a retry of this DAG re-enters the same launcher
run and its Nextflow launch directory, and -resume skips what already completed. The launcher
prefixes its own qc- to it, which keeps it apart from post-processing.

The tag also drops the double underscore after the run type, so a scheduled run id becomes
scheduled- rather than scheduled with two underscores. Airflow 3.2 refuses an
operator-triggered run whose id carries the scheduled prefix, since that is reserved for
scheduled runs, and reports it as an opaque 500 from the API server.
