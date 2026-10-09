# Case Status Control

The single scheduled entry point for case processing (**daily**, one run at a time). It moves
the cases with work waiting to `processing`, runs the pipelines, then moves to `in_progress` the
cases that every pipeline is done with and that have variant data.

```
discover_{snv,cnv,qc,import}_cases, fetch_case_statuses
    -> discover_cases -> set_processing (one per tenant)
    -> trigger_snv_postprocessing -> trigger_import --+
    -> trigger_cnv_postprocessing --------------------+-> rediscover_{snv,cnv,qc,import}_cases,
    -> trigger_quality_control -----------------------+   refresh_case_statuses
    -> select_ready_cases -> evaluate_cases (one per tenant)
```

The three "from Cases" DAGs have no schedule of their own: this DAG is their schedule. They can
still be run by hand for a targeted rerun, which leaves the case statuses alone.

## The only status changes

| From | To | When | Task |
|---|---|---|---|
| `submitted` | `processing` | the case has work waiting, or already has variant data | `set_processing` |
| `processing` | `in_progress` | no pipeline has work waiting for the case any more, and it has variant data | `evaluate_cases` |

Each change is sent to `PATCH /{tenant}/cases/status` with the status it is expected to be in.
The portal checks it under a row lock: a case a geneticist moved in the meantime comes back
`updated: false` with its current status, is logged, and is left alone. Every other status
(`draft`, `in_review`, `completed`, `revoked`...) is never touched.

## What counts as "work waiting"

The union of:

- the cases each "from Cases" DAG would run: its own `pending_*_select.sql` query and its own
  `select_cases`, with the tenant allow-list that DAG uses, plus, for QC, its S3 probe for the
  DRAGEN metrics (`locate_metrics`, under `$NEXTFLOW_INPUTS_ROOT`). A case that DAG would exclude
  (sequencing pending, no gVCF, tenant not granted, metrics not found...) is **not** counted: it
  would sit in `processing` with nothing running for it, and never reach `in_progress`;
- the cases in the import delta view, `staging_sequencing_experiment_delta`.

The question is asked twice with the same queries: before the pipelines (`discover_*`), to pick
the cases to move to `processing`, and after them (`rediscover_*`), to hold back the cases a
pipeline still has work for.

The `submitted` cases that already have variant data are moved to `processing` too: work done
before this DAG saw the case (a manual rerun, data imported before the statuses existed) would
otherwise leave it `submitted` for ever. The same run's `evaluate_cases` moves them on once
nothing is pending for them.

## Variant data

`has_variants`, in `sql/clinical/case_status_select.sql`: at least one experiment of the case
has been imported (`ingested_at` set by `import_part`) from an SNV or CNV VCF, and is not deleted.

## Failures do not block the data

Every step after `set_processing` runs with `trigger_rule=all_done`:

- on a day with no `submitted` case `set_processing` is skipped, and the cases already in
  `processing` or later with new files must still be processed;
- a portal outage fails `set_processing`, and the pipelines still run;
- a failed pipeline still lets the import and `evaluate_cases` run.

`evaluate_cases` takes **every** `processing` case, not only this run's, and holds back any case
the second discovery still finds. A case whose pipeline failed stays `processing`, even with some
variants in (say its SNV post-processing failed but the alignment's CNV VCF was imported), and is
moved by the later run that finishes it. A case with nothing pending but no variant data stays
`processing` too, and is logged as a warning by `select_ready_cases`.

Since the `all_done` chain would otherwise end green, `watcher` fails the run as soon as any step
failed. Look at the failed task and at the child runs.

## Tenants

Status changes are sent once per tenant, as one mapped task each. A tenant missing from
`status_tenants` keeps its cases' statuses and is logged. A tenant the portal refuses with a 403
(the service account is not granted `can_ingest_data` at every lab, `'*'`, of that tenant) is
logged and its task **skipped**, not failed.

## Notification

The laboratories are emailed when their cases reach `in_progress`, not when the pipelines
register their outputs: the manifest then holds every pipeline's results. The "from Cases" DAGs
no longer email. `evaluate_cases` is a mapped task group, one chain per tenant, so a tenant
skipped or failed never holds back another tenant's email:

- **set_in_progress** sends the status change;
- **create_case_group** POSTs `/{tenant}/case_groups` with the cases now in `in_progress` under
  the name `results-<run tag>` (the labs see it in the manifest's file name). It counts the cases
  an earlier attempt of the same task moved too, and is skipped when no case moved. The name is
  the key and the call overwrites, so a retry lands on the same group;
- **notify_labs** POSTs `/{tenant}/case_groups/{name}/notify`. The portal emails each diagnosis
  laboratory the TSV manifest of its cases' documents, read at sending time. Skipped when
  `notify` is false.

The log holds one line per laboratory:

| Status | Meaning | Task outcome |
|---|---|---|
| **sent** | Email sent with the manifest attached | success |
| **skipped_no_contact** | The organization has no `notification_emails` | warning, success |
| **skipped_no_documents** | Nothing registered for that laboratory's cases | warning, success |
| **failed** | The SMTP relay refused the email | task fails |

> **A retry re-sends.** The portal keeps no record of what was emailed: clearing **notify_labs**
> emails every laboratory of that tenant again. Clear one mapped instance, not the whole group.

A **500** means nothing was sent: the tenant has no notification template mounted, or the SMTP
settings are invalid. The manual **notify_cases** DAG re-sends an existing group.

A case already `in_progress` (or later) that gets new data is processed and imported, but keeps
its status, so it is **not** emailed again.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `status_tenants` | `$NEXTFLOW_POSTPROCESSING_TENANTS` | Tenants the service account may change statuses in. Empty means no filtering. |
| `notify` | **true** | Email the laboratories of the cases moved to `in_progress`. False still creates the case group, so **notify_cases** can send later. |

Not `tenants`: the discovery tasks set that one to each "from Cases" DAG's own allow-list
(`$NEXTFLOW_POSTPROCESSING_TENANTS`, `$NEXTFLOW_CNV_TENANTS`, `$NEXTFLOW_QC_TENANTS`), and a run
conf key of that name would override them.

## Child runs

The four child run ids are pinned to this run (`sanitize_run_tag(run_id)`), so a retry resets the
same child run and Nextflow's `-resume` still finds its launch directory. The tag drops the `__`
after the run type: Airflow 3.2 refuses a triggered run whose id starts with `scheduled__`.

## Portal connection

Uses the Airflow Connection `radiant_api_conn`, same as the "from Cases" DAGs: `host` = the API
base url, `login` / `password` = the OIDC client credentials, `extra` =
`{"token_url": "...", "scope": "..."}`.
