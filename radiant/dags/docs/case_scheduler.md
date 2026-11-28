# Case Scheduler

The single scheduled entry point for case processing (**daily**, one run at a time). It starts the pipelines, then
the import. It **decides nothing and moves no status**.

```
trigger_germline_snv_postprocessing ---+
trigger_somatic_snv_annotation --------+-> trigger_import
trigger_germline_cnv_postprocessing ---+
trigger_quality_control
watcher (fails the run when any step failed)
```

## Who does what

| Step | Done by |
|---|---|
| Decide each item, journal it (**pipeline_journal**) | each pipeline DAG at its start (**radiant.dags.pipeline_decision**), and **radiant-import** |
| **submitted** → **processing** | the pipeline DAG that runs work on the case (germline SNV, germline CNV, somatic SNV) and **radiant-import** for the cases it loads. Not quality control: it does not count for the statuses yet |
| **processing** → **in_progress**, lab email | **radiant-case-release**, started by **radiant-import** with the cases it loaded |

Why an item does or does not run is in **pipeline_journal**, per item, and **pipeline_state** gives the last row of
each.

## Order and failures

The four pipelines run in parallel, **all_done**: a failed one never holds the others back. The import waits for
germline SNV post-processing, somatic SNV annotation and germline CNV post-processing (the slivar VCF, RAD-206),
whatever their outcome, since it loads the annotation tasks they register. Quality control is not upstream of it
yet.

A failure holds back only the cases it concerns: **radiant-case-release** leaves a case **processing** while one
of its germline SNV, germline CNV or somatic SNV items is still running or failed, and a later run that finishes it
moves it. **watcher** turns the run red when any step failed, since the **all_done** steps would otherwise let it end
green. Look at the failed task and at the child runs.

## Tenants

**excluded_tenants** (default **$RADIANT_ETL_EXCLUDED_TENANTS**, comma separated, empty excludes none) is passed to
every DAG it starts: the pipelines journal their cases as **tenant_excluded** and do not run them, and no status of
theirs moves. The import loads every tenant's data, but leaves the excluded tenants' statuses alone.

A tenant that the portal refuses with a 403 (the service account is not granted **can_ingest_data** at every lab,
**'*'**, of that tenant) has its status moves **skipped**, not failed.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| **excluded_tenants** | **$RADIANT_ETL_EXCLUDED_TENANTS** | Tenants left out of case processing, see Tenants |
| **notify** | **true** | Passed to the import, then to **radiant-case-release**: email the laboratories. False still creates the case group, so **notify_cases** can send later |

## Child runs

The five child run ids are pinned to this run (**sanitize_run_tag(run_id)**), so a retry resets the same child run
and Nextflow's **-resume** still finds its launch directory, and every journal row of the run shares one run tag. The
tag drops the **\_\_** after the run type: Airflow 3.2 refuses a triggered run whose id starts with **scheduled\_\_**.
