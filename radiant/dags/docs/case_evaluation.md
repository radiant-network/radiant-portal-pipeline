# Case Evaluation

Moves the named cases that are ready to **in_progress** and emails their laboratories. **radiant-import** triggers
it at its end with the cases it loaded new data for (an item **succeeded** after **first_run** or **retry**); an
operator can run it by hand with **case_ids**, to evaluate or email again.

has_cases → fetch_case_statuses, fetch_pipeline_state → select_ready_cases → evaluate_cases (one per tenant:
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
