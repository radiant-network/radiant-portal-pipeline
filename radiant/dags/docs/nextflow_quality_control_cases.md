# Nextflow Quality Control (from Cases)

Finds the germline cases that have been aligned but never quality-controlled, runs the Ferlab
[quality-control-pipeline](https://github.com/Ferlab-Ste-Justine/quality-control-pipeline)
over them from their DRAGEN metrics, and registers the per-family MultiQC report back onto
each case as a **quality_control_metrics** task.

**radiant-nextflow-quality-control** runs the pipeline and nothing else. This DAG closes
everything around it, exactly as **radiant-nextflow-snv-postprocessing-cases** does for
annotation.

| # | Task | Does |
|:--|:--|:--|
| 1 | **discover_scope** | Establishes what this run is allowed to look at |
| 2 | **select_cases** | Finds eligible cases, excludes the rest with a reason |
| 3 | **resolve_cases** | Builds one family per case, proband first |
| 4 | **generate_inputs** | Writes one samplesheet for every case of the run |
| 5 | **run_pipeline** | One launcher run, for every case |
| 6 | **collect_outputs** | Lists what the pipeline published |
| 7 | **register_tasks** | PATCHes the portal, one mapped instance per tenant |

It has **no schedule of its own and takes no input**: `radiant-case-scheduler` starts it
daily with its own `excluded_tenants`, and moves the case statuses around it. Run
it by hand only for a targeted rerun: the `excluded_tenants` default below applies to a manual run only. The
parameters exist for targeted reruns and for configuration, not for normal operation.

---

## Parameters

| Param | Default | Meaning |
|:--|:--|:--|
| **task_ids** | empty | alignment_germline_variant_calling task ids, for a targeted rerun. Empty means "find everything" |
| **excluded_tenants** | $RADIANT_ETL_EXCLUDED_TENANTS | Tenants left out of case processing, reported as **tenant_excluded**. Empty excludes none |
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

The query returns **one row per member**, with its CRAM, its index and its DRAGEN metrics
tarball.

---

## Where the DRAGEN metrics come from

Each samplesheet row carries its sample's DRAGEN metrics in the **dragenMetrics** column, and
the pipeline reads them instead of recomputing BAM_QC and VCF_QC for that sample:

- **The alignment's tarball**, when the current alignment published an output with data type
  **ssup** and format **tgz**. It is selected on those type fields, the way the CRAM is, never
  on the file name. The pipeline extracts it in its **UNTAR** step.
- **Otherwise, the CRAM's directory.** This fallback is not checked before the run.
  **generate_inputs** logs a warning for each such member, with the directory; grep for
  "no DRAGEN metrics tarball".

> **dragenMetrics is never empty.** An empty value makes the pipeline recompute BAM_QC and
> VCF_QC for that sample, hours of worker pods nobody asked for.

The pipeline keeps only the files whose **exact first dot-token** is the row's sample, and
drops the others with a warning in its log. That is why the samplesheet's **sample** column is
the **aliquot**: that is what DRAGEN names its files after. It also means a fallback directory
without that sample's metrics **fails open**: a green run with empty alignment and coverage
sections.

Since every sample brings its own metrics, nothing splits the cases: **every case of the run
goes into one launcher run**. One bad case fails that run for all of them; a retry resumes
what already completed.

---

## Nothing fails the run — it gets excluded

A candidate that cannot be run is dropped with a reason and the rest continues.
**select_cases** logs them; grep for "excluded".

| Reason | Meaning | Transient? |
|:--|:--|:--|
| **pending_sequencing** | A member has no completed sequencing experiment yet | Yes |
| **pending_alignment** | A member's current experiment has no alignment task yet | Yes |
| **no_cram** | The current alignment published no CRAM, and somalier needs one | No |
| **ambiguous_cram** | More than one CRAM on one alignment task — a mistyped document | No, fix the data |
| **ambiguous_dragen_metrics** | More than one ssup / tgz tarball on one alignment task | No, fix the data |
| **proband_count** | Two different patients are marked proband on the case | No, fix the data |
| **unsupported_strategy** | A strategy outside {wgs, wxs, wes} | No |
| **no_project_code** | cases.project_id did not resolve, and the batch PATCH needs it | No |
| **tenant_excluded** | The tenant is in **excluded_tenants** | No, by configuration |

A member with no tarball is **not** excluded: it falls back to its CRAM's directory.

If every candidate is excluded the run **skips**. A skipped run is not a failed one.

> **A targeted rerun behaves differently on purpose.** With **task_ids** set, an unresolvable
> case fails the run instead of being excluded.

---

## What gets registered

One **quality_control_metrics** task per case, bound to every member's aliquot, with the
alignment's CRAM, index and DRAGEN metrics tarball (when there is one) as input documents and,
as outputs, everything the pipeline published for the family under its multiqc/CA directory:

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
| **NEXTFLOW_INPUTS_ROOT** | — | **generate_inputs** writes here. An s3:// uri, and the CRAMs and tarballs must also be under this bucket |
| **NEXTFLOW_OUTPUTS_ROOT** | — | **collect_outputs** lists here. An s3:// uri |
| **NEXTFLOW_INPUTS_MOUNT** | /workspace/inputs | The pod path the inputs bucket appears at |
| **NEXTFLOW_OUTPUTS_MOUNT** | /workspace/outputs | The pod path the outputs bucket appears at |
| **RADIANT_ETL_EXCLUDED_TENANTS** | empty | Comma-separated default for the **excluded_tenants** param, shared with the case scheduler and the other cases DAGs |

Each run gets its own subdirectory, named after the run:

- INPUTS_ROOT / qc-runs / RUN_TAG / samplesheet.csv
- OUTPUTS_ROOT / qc / RUN_TAG / multiqc / CA_ID /

The qc-runs/ and qc/ subdirectories keep these apart from post-processing, which writes
snv-runs/ and snv/ under the same two roots.

The child run id is pinned to RUN_TAG, so a retry of this DAG re-enters the same launcher
run and its Nextflow launch directory, and -resume skips what already completed. The launcher
prefixes its own qc- to it, which keeps it apart from post-processing.

The tag also drops the double underscore after the run type, so a scheduled run id becomes
scheduled- rather than scheduled with two underscores. Airflow 3.2 refuses an
operator-triggered run whose id carries the scheduled prefix, since that is reserved for
scheduled runs, and reports it as an opaque 500 from the API server.
