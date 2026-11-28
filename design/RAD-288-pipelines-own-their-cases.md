# RAD-288 Each pipeline decides its cases, and the import's end moves them to in_progress

Story [RAD-286](https://ferlab-crsj.atlassian.net/browse/RAD-286), sub-task RAD-288 (decision journal). Builds on the import journal (#381) and the task lineage (`output_task_id`).

**Summary:** the case scheduler stops deciding and stops moving statuses: it triggers the pipelines, then the import. Each pipeline decides its items, journals them, moves its `run` cases from `submitted` to `processing`, and runs. At its end the import triggers a new DAG, `radiant-case-release`, with the cases it loaded: that DAG moves the ready ones to `in_progress` and emails the laboratories.

## 1. Why

- A Nextflow pipeline is decided twice and sometimes three times: by the scheduler before the pipelines (to pick the cases to move to `processing`), by the child DAG (lenient re-check of the cases it is given), and by the scheduler again after the pipelines (`rediscover_*`, to hold back the cases with work left). The import is already decided by itself (#381), and the scheduler decides it again only to count.
- The decisions of a Nextflow pipeline are journaled by the scheduler, its outcomes by the child, under two run ids until the run tag was unified. A child run by hand journals outcomes and no decision.
- `processing` → `in_progress` waits for the whole run: one failed import, CNV post-processing or QC (`none_failed`) holds back every case of every tenant, and their emails.
- The cases emailed while already past `in_progress` come from the scheduler's discovery at the start of the run, not from what the run actually loaded.

## 2. Decided

- The scheduler decides nothing and changes no status.
- Whoever runs work on a case moves it to `processing`, right after its decision: germline SNV, germline CNV, somatic SNV and the import. Not QC: QC does not count for the statuses yet (it will move upstream of the import later), and a case moved by QC alone would have nothing to bring it to `in_progress`.
- `processing` → `in_progress` and the lab email are triggered by the end of the import, in a separate DAG the import triggers, given the cases.
- `processing` stays. No motive in the email yet. No automatic resend of a failed email: `notify_cases` stays the manual resend.

## 3. The run

```
radiant-case-scheduler (@daily)
  ├─ trigger germline SNV, germline CNV, somatic SNV, QC  (all_done)
  └─ trigger import                                        (after the first three, all_done)

each "from Cases" DAG and the import
  facts → decide → journal → move `run` cases to processing → run → outcomes

radiant-import, after its parts
  outcomes (journal) → trigger radiant-case-release(case_ids = cases loaded by this run)

radiant-case-release
  read the statuses and pipeline_state of the named cases
  → drop the cases not ready
  → per tenant: processing → in_progress, case group, email
```

## 4. Changes by DAG

### Case scheduler (`case_scheduler.py`)

- Keeps the four triggers and the import trigger, their order and `all_done`, and `excluded_tenants` in the children's conf. Passes no `case_ids` / `task_ids`.
- Removes: `discover_*`, `decide_*`, `journal_*`, `fetch_case_statuses`, `discover_cases`, `set_processing`, `rediscover_*`, `refresh_case_statuses`, `select_ready_cases`, `release_cases`. `watcher` stays.
- `case_status.py`: `cases_with_work`, `plan_processing` and the scheduler's use of `DECIDE` go; `plan_in_progress` and the per-tenant helpers move to the release.

### Nextflow "from Cases" DAGs (germline SNV, germline CNV, somatic SNV)

- New head, before the existing selection:
  - `fetch_facts`: the facts query the scheduler reads today (`sql/clinical/<pipeline>_facts_select.sql`), every candidate;
  - `decide`: `DECIDE[pipeline]`, with the run's `excluded_tenants`;
  - `journal`: `record()`;
  - `set_processing`: the cases decided `run`, per tenant, moved to `processing` (`change_tenant_status`; the portal only moves `submitted` ones, a 403 skips the tenant).
- The existing selection then takes the cases (or calling tasks) decided `run`, instead of `params.case_ids`.
- `case_ids` / `task_ids` stay as optional params: an operator names the items to run. Every candidate is still decided and journaled; only the named ones decided `run` are run and moved. Empty runs every item decided `run` (a change for somatic SNV: without `task_ids` it used to run nothing).
- `has_cases` goes: a run with no candidate stops after the journal (`short_circuit` on the run list).

### QC "from Cases" DAG

- Same head (both facts queries, two pipelines in the journal), without `set_processing`.

### Import (`import_radiant.py`)

- `set_processing` after `decide_import`, for the cases decided `run` whose reason is `first_run` or `retry` (a purge, `task_deleted`, brings no data to analyse). Needs `excluded_tenants` (new param, passed by the scheduler): the import loads every tenant, but statuses skip the excluded ones.
- `journal_import_outcomes` stops being a teardown: an `all_done` task that journals the outcomes and returns the cases with an item `succeeded` whose decision was `first_run` or `retry`.
- `trigger_case_release`: `TriggerDagRunOperator` to `radiant-case-release`, conf `{case_ids, excluded_tenants, notify}`, not waiting for completion; skipped when no case. Its failure does not fail the import (same handling as `data_integrity_checks`).

### New `radiant-case-release`

- Params: `case_ids` (required), `excluded_tenants`, `notify`. Manual runs re-check or resend for named cases.
- `fetch_case_statuses`: `case_status_select.sql`, limited to `case_ids`.
- `fetch_pipeline_state`: the last row of the named cases' items in `pipeline_state`.
- `select_ready_cases`: a case is ready when none of its `germline_snv`, `germline_cnv`, `somatic_snv` items has a last row `failed`, or `run` (no outcome yet). Ready and `processing` → moved to `in_progress`; ready and past `processing` → emailed only, status unchanged; `submitted` or not ready → left alone, logged.
- `release_cases`: the current mapped task group per tenant (`set_in_progress` → `create_case_group` → `notify_labs`), moved as is.

## 5. Behaviour changes

- A failure holds back the cases it concerns only: no run-wide `none_failed` gate on the statuses.
- A case is emailed when the import loads something for it, once its other pipelines are not failed or running. The QC report alone no longer triggers an email.
- A child or import run by hand moves and releases its cases like a scheduled one.
- A `submitted` case with variants loaded outside the pipelines (the old `plan_processing` rule) is no longer moved: no pipeline runs for it. To revisit if it happens.
- A case moved to `processing` whose pipeline then fails stays `processing` until a later run succeeds, as today.

## 6. Edge cases

- **Partial family, member pending:** the pipeline runs the present members (`wait` item for the absent one is not a blocker); the case reaches `in_progress` when the import loads, as today.
- **Blocked item** (no gCNV, missing value): does not hold the case back, as today.
- **Item decided `run` by the child but the import blocked** (`missing_input`): no import `succeeded`, no release, the case stays `processing`; the journal says why.
- **Facts move between the child's decision and the import:** none of the decisions is shared any more, so there is nothing to reconcile.

## 7. Commits (on `feat/rad-288-task-lineage`)

1. `radiant-case-release` DAG, its selection rule from `pipeline_state`, and the import's outcomes task + trigger.
2. Germline SNV child: facts → decide → journal → `processing`.
3. Germline CNV child, then somatic SNV, then QC (without `processing`).
4. Import `set_processing` and `excluded_tenants`.
5. Scheduler: remove the decisions, statuses and release; `case_status.py` clean-up; docs (`case_scheduler.md`, each DAG doc, `CLAUDE.md`).

## 8. Tests

- Unit: the readiness rule (failed, run without outcome, succeeded, wait, blocked; QC ignored); the import's list of cases to release (`task_deleted` excluded); each child's new head (named items filter execution, not the decision); the scheduler's task list and wiring; the release DAG's wiring.
- Integration: `pipeline_state` read for named cases; `case_status_select.sql` limited to `case_ids`.

## 9. Upgrade

- No migration. Deploy note: `radiant-case-release` is new and must be unpaused; the scheduler no longer moves statuses, so the first run after the deploy moves cases only through the new flow.

## 10. Later

- Automatic resend of a failed email (`lab_notification` item in the journal).
- The motive per case in the email and the portal ("father added"), from the journal and `output_task_id`.
- QC upstream of the import, then counted for the statuses.
- Dropping `processing`: the portal reads the ETL state from the journal.
