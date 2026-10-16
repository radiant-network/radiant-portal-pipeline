# Nextflow Somatic SNV Annotation (from Cases)

Finds the somatic calling tasks whose raw SNV VCF was never annotated, annotates it with the same
VEP as germline, and registers the result on the case as a **radiant_somatic_annotation** task.
The scheduled import already loads that task: nothing changes there.

The launcher is **radiant-nextflow-somatic-snv-annotation**: the germline pipeline, image and
**nextflow-snv-cfg**, with its own params ConfigMap. It enters the Ferlab
[snv-post-processing](https://github.com/Ferlab-Ste-Justine/snv-post-processing) at its
**normalize** step, with **tools = vep**: split multi-allelic records, left-align indels on the
reference, then VEP. No genotyping, slivar or Exomiser. The loader skips multi-allelic records, so
without the split those variants would be lost.

| Step | Does |
|:--|:--|
| **discover_scope** | Runs the pending query |
| **select_tasks** | Keeps what can run, reports what cannot, skips the run if nothing can |
| **generate_inputs** | Checks the raw VCFs exist, writes the samplesheet |
| **run_pipeline** | Triggers **radiant-nextflow-somatic-snv-annotation**, and waits |
| **collect_outputs** | Finds the VEP VCF and its index of every calling task |
| **list_tenants**, **register_tasks** | One batch PATCH per tenant |

It has **no schedule of its own**: **radiant-case-scheduler** starts it daily with its own
excluded tenants, moves the case statuses around it, and starts the scheduled import after it,
success or not. Run it by hand only for a targeted rerun.

---

## Parameters

| Param | Default | Meaning |
|:--|:--|:--|
| **task_ids** | empty | Calling task ids, for a targeted rerun, e.g. [70]. Empty means "find everything". |
| **excluded_tenants** | $RADIANT\_ETL\_EXCLUDED\_TENANTS | Tenants left out, reported as **tenant_excluded**. Empty excludes none. |
| **dry_run** | **false** | Passed to the batch PATCH. True validates and writes nothing. |

> **dry_run on a scheduled run is a trap.** Nothing is registered, so every calling task stays
> pending and the next run does the same work again. Manual runs only.

---

## What counts as "needs somatic annotation"

The unit of work is the **calling task**, not the case. A tumor-normal and a tumor-only call on
the same tumor sample are two VCFs, and both are annotated.

A calling task (**somatic_variant_calling**, **tumor_only_variant_calling** or
**alignment_somatic_variant_calling**) is pending when:

- it published an output document with data type **ssnv** and format **vcf**, selected on these
  type fields, never on the file name. The **scnv** VCF of the same task is never selected;
- no **radiant_somatic_annotation** registered on that case has that document as input. Per
  case: a task held by two cases, registered on one only (say the other tenant's PATCH failed),
  is still pending on the other;
- a somatic case, not draft or revoked, holds **every** sequencing experiment of the task. The
  portal refuses an input document from an experiment the case does not hold.

The rule lives in **sql/clinical/pending_somatic_annotation_select.sql**. A calling task sent the
old way (no **ssnv** VCF, annotation from QLIN) is not a candidate, and is left as it is.

---

## What the pipeline is given

One samplesheet row per calling task:

| Column | Value |
|:--|:--|
| **familyId** | the calling task id. It names the output: ensemblvep/variants.TASK\_ID.snv.vep.vcf.gz |
| **sequencingType** | **WGS** or **WES**, from the experiments' strategy (wxs gives WES) |
| **vcf** | the raw **ssnv** VCF, as a pod path |

The VCF must be in the bucket of NEXTFLOW\_INPUTS\_ROOT: it is read off the FSx mount.
**generate_inputs** fails, naming every calling task and URL, when a VCF is outside that bucket
or missing. Nothing is run then.

---

## Nothing fails the run, it gets excluded

A candidate that cannot be run is dropped with a reason, and the rest continues.
**select_tasks** logs them: search the log for **excluded**.

| Reason | Meaning | Scope |
|:--|:--|:--|
| **pending_sequencing** | an experiment of the task is not **completed** yet (transient) | the task |
| **no_ssnv** | the task published no **ssnv** VCF. Only reported for a task named in **task_ids** | the task |
| **ambiguous_ssnv** | more than one **ssnv** VCF on one task: a mistyped document | the task |
| **missing_aliquot** | an experiment of the task has no aliquot | the task |
| **unsupported_samples** | not one tumoral sample plus at most one normal one, or more than one aliquot on an **alignment_somatic_variant_calling** | the task |
| **unsupported_strategy** | a strategy outside wgs, wxs, wes, or experiments spanning several | the task |
| **no_project_code** | the case's project did not resolve; the batch PATCH needs it | that case only |
| **tenant_excluded** | the case's tenant is in **excluded_tenants** | that case only |

If everything is excluded, the run **skips**. A skipped run is not a failed one.

**A targeted rerun behaves differently on purpose.** With **task_ids** set, a task that cannot be
resolved fails the run instead of being excluded.

---

## What gets registered

One **radiant_somatic_annotation** task per calling task and case, through
PATCH /TENANT/cases/batch:

| Field | Value |
|:--|:--|
| **aliquots** | the calling task's own: the tumor, plus the normal for tumor-normal. The portal derives the task context from them, so the TO/TN classification of the import is the calling task's |
| **input_documents** | the raw **ssnv** VCF, already in the tenant. This link is also what makes the task stop being pending |
| **output_documents** | the VEP VCF and its index: **ssnv** / **vcf** and **ssnv** / **tbi** |
| **pipeline_name**, **pipeline_version** | **snv-post-processing** and its pinned revision, as for germline. The revision pins the VEP version |

**collect_outputs** requires the complete set for every calling task of the run: a partial run
registers nothing. PATCH **appends**: a deliberate re-run adds a second annotation task.

---

## Where things are written

| Path | Holds |
|:--|:--|
| NEXTFLOW\_INPUTS\_ROOT / somatic-snv-runs / RUN\_TAG / samplesheet.csv | the samplesheet |
| NEXTFLOW\_OUTPUTS\_ROOT / somatic-snv / RUN\_TAG / ensemblvep / | the VEP VCFs and their indexes |

RUN\_TAG is **somatic-** plus the run id. The prefix is load-bearing: the case scheduler gives
every cases DAG the same run id, and the Nextflow launch and work directories are keyed by the run
tag on a filesystem every launcher shares. Without it, this run and the germline SNV one would
share both, and each one's cleanup would delete the other's scratch.

The child run id is pinned to RUN\_TAG, so a retry of this DAG re-enters the same launcher run,
and Nextflow's resume skips what already completed.

The launcher reads the **nextflow-snv-somatic-params** ConfigMap (qlin-qa-infra), next to the
germline **nextflow-snv-params**. Both share **nextflow-snv-cfg**. Without that ConfigMap the
driver pod does not start.

---

## Portal connection

Registration uses the Airflow Connection **radiant_api_conn**, as the other cases DAGs do.
