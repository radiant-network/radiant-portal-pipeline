# CLIN-6227: Lab manifest notification from the post-processing DAG

https://ferlab-crsj.atlassian.net/browse/CLIN-6227

Radiant's Airflow, not a worker, triggers the laboratory notification once a run's documents are
registered. The mechanism (case groups, the `notify` endpoint, templates, SMTP) lives in
radiant-api; its design is `design/manifest-notification.md` in the radiant-portal repository.
This note only covers the pipeline side.

## Decisions

| Topic | Decision |
|:--|:--|
| Where | Two mapped tasks after `register_tasks` in `nextflow-postprocessing-cases`, one per tenant, in the same `ingest_data` grant and `radiant_api_conn` connection. No new client, no new grant. |
| Group name | `postprocessing-<run tag>` (`sanitize_run_tag(run_id)`): dated, unique per run, stable across retries, so `POST /case_groups` overwrites its own group on a retry. |
| Ordering | `register_tasks >> create_case_group >> notify_labs` at task level. A manifest built before the PATCH landed would be empty. |
| Failure policy | `notify_labs` fails only when a laboratory is `failed` (relay refused) or the portal answers 500 (template or SMTP missing, infra). `skipped_no_contact` / `skipped_no_documents` are warnings: an admin task and an empty run respectively, not pipeline errors. |
| Params | `notify` (default true) skips `notify_labs` only; `dry_run` skips both. |
| Re-sends | Manual DAG `notify-cases`: existing group by name, or an ad-hoc `case_ids` list grouped under `manual-<run tag>`. Every run sends, the portal keeps no memory. |
| Client | `radiant/tasks/nextflow/portal.py` gains `post_case_group` / `notify_case_group` next to `patch_case_batch`; connection loading is shared through `register.load_portal_connection`. |
