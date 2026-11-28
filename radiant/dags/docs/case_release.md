# Case Release

Moves the named cases that are ready to **in_progress** and emails their laboratories. **radiant-import** triggers
it at its end with the cases it loaded new data for (an item **succeeded** after **first_run** or **retry**); an
operator can run it by hand with **case_ids**, to release or email again.

has_cases → fetch_case_statuses, fetch_pipeline_state → select_ready_cases → release_cases (one per tenant:
set_in_progress → create_case_group → notify_labs)

## Ready

A case is ready when none of its **germline_snv**, **germline_cnv** or **somatic_snv** items has, as its last journal
row (**pipeline_state**), **run** (still running, no outcome yet) or **failed**. **wait**, **blocked** and **skip** do
not hold it back. Quality control does not count yet.

| Case | Ready | Not ready |
|---|---|---|
| **processing**, with imported variants | moved to **in_progress**, emailed | left **processing** |
| past **processing** (**in_progress**, **in_review**...) | emailed, status unchanged | left alone |
| **submitted** | left alone | left alone |

A case not ready is not lost: the import that later loads the missing data names it again.

## Tenants

**excluded_tenants** (default **$RADIANT_ETL_EXCLUDED_TENANTS**) are left alone. A tenant not granted to the service
account gets a 403 from the portal and is skipped.

## Email

One case group per run and tenant, **results-&lt;run tag&gt;**. With **notify** off, the group is created but no
email goes: **notify_cases** sends it later. A failed email is not sent again by itself: use **notify_cases**.

**release_cases** is a mapped task group, one chain per tenant, so a tenant skipped or failed never holds back
another tenant's email:

- **set_in_progress** sends the status change; no call for a tenant with nothing to move;
- **create_case_group** POSTs **/{tenant}/case_groups** with the cases now **in_progress** and the ones to notify
  unchanged. The name is the key and the call overwrites, so a retry lands on the same group;
- **notify_labs** POSTs **/{tenant}/case_groups/{name}/notify**: the portal emails each diagnosis laboratory the TSV
  manifest of its cases' documents. Skipped when **notify** is false.

| Status | Meaning | Task outcome |
|---|---|---|
| **sent** | Email sent with the manifest attached | success |
| **skipped_no_contact** | The organization has no **notification_emails** | warning, success |
| **skipped_no_documents** | Nothing registered for that laboratory's cases | warning, success |
| **failed** | The SMTP relay refused the email | task fails |

> **A retry re-sends.** The portal keeps no record of what was emailed: clearing **notify_labs** emails every
> laboratory of that tenant again. Clear one mapped instance, not the whole group.

A **500** means nothing was sent: the tenant has no notification template mounted, or the SMTP settings are invalid.

## Portal connection

The Airflow Connection **radiant_api_conn**, as the pipeline DAGs: **host** = the API base url, **login** /
**password** = the OIDC client credentials, **extra** = **{"token_url": "...", "scope": "..."}**.
