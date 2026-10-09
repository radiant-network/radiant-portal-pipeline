# Notify Laboratories (case group)

Emails each diagnosis laboratory of a case group the TSV manifest of its output documents,
through the portal's `POST /{tenant}/case_groups/{name}/notify`. This is the manual counterpart
of the **notify_labs** step of the case status control DAG: use it to send a run's notification
again, or to notify an ad-hoc set of cases.

**Every run sends.** The portal keeps no record of earlier emails, so triggering this twice on
the same group emails the laboratories twice.

## Parameters

| Param | Default | Meaning |
|:--|:--|:--|
| **tenant** | required | Tenant code of the group |
| **case_group_name** | `manual-<run tag>` | Group to notify. The case status control DAG names its groups `results-<run tag>` (groups from before it are `postprocessing-<run tag>`) |
| **case_ids** | empty | When given, the group is created (or its case list overwritten) with these cases before notifying |

## What the report says

The **notify_labs** log holds one line per laboratory:

| Status | Meaning | Task outcome |
|:--|:--|:--|
| **sent** | Email sent with the manifest attached | success |
| **skipped_no_contact** | The organization has no `notification_emails`; an admin sets it through `PUT /organizations/{code}` | warning, success |
| **skipped_no_documents** | The laboratory's cases have no output document yet | warning, success |
| **failed** | The SMTP relay refused the email; the error is on the line | task fails |

Each line also carries the template file the portal rendered and the context it rendered it
with (`has_stat`, `analysis_codes`, `case_ids`, `manifest_filename`).

A **500** from the portal means nothing was sent: the tenant has no notification template
mounted, or the SMTP settings are invalid. Both are infrastructure configuration, not pipeline
state. A **404** means the group does not exist in that tenant.

## Authentication

Same **radiant_api_conn** connection and `ingest_data` grant as the case status control DAG and
the post-processing DAG's registration step; see their documentation.
