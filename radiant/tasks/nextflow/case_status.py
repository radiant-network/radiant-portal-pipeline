"""Which cases the pipelines move, and to what.

The ETL only ever makes two status changes: `submitted` -> `processing`, by the pipeline DAG that
decides to run work on a case (`decide_run`), and `processing` -> `in_progress`, by
`radiant-case-release` once the import has loaded the case's data and no pipeline is still
running or failed for it (`not_ready_cases`, `plan_in_progress`). Everything here
decides *which* cases; `portal.set_case_system_status` sends the change and the portal checks
the expected status under a row lock, so a case a geneticist moved in the meantime is left
alone.

Airflow is imported inside `change_tenant_status` only, like `register.py`: the rules stay
importable from plain unit tests.
"""

import logging
from collections.abc import Callable

from pydantic import BaseModel

from radiant.tasks.decision import Decision
from radiant.tasks.journal import Entry, Pipeline
from radiant.tasks.nextflow.cnv.resolve import decide_cases as decide_cnv_cases
from radiant.tasks.nextflow.qc.resolve import decide_cases as decide_qc_cases
from radiant.tasks.nextflow.qc.resolve import decide_somatic_cases as decide_qc_somatic_cases
from radiant.tasks.nextflow.resolve import decide_cases as decide_snv_cases
from radiant.tasks.nextflow.somatic.resolve import decide_tasks as decide_somatic_snv_tasks

LOGGER = logging.getLogger(__name__)

# The pipelines decided from their facts, by the DAG that runs them (`radiant.dags.pipeline_decision`).
DECIDE: dict[Pipeline, Callable[[list[dict], list[str]], list[Entry]]] = {
    Pipeline.GERMLINE_SNV: decide_snv_cases,
    Pipeline.GERMLINE_CNV: decide_cnv_cases,
    Pipeline.QUALITY_CONTROL: decide_qc_cases,
    Pipeline.QUALITY_CONTROL_SOMATIC: decide_qc_somatic_cases,
    Pipeline.SOMATIC_SNV: decide_somatic_snv_tasks,
}

# The pipelines whose items hold a case back from `in_progress`. Not QC: it does not count for the statuses yet.
READINESS_PIPELINES = (Pipeline.GERMLINE_SNV, Pipeline.GERMLINE_CNV, Pipeline.SOMATIC_SNV)

SUBMITTED = "submitted"
PROCESSING = "processing"
IN_PROGRESS = "in_progress"


class CaseStatus(BaseModel):
    case_id: int
    tenant_code: str
    status_code: str
    has_variants: bool


def plan_in_progress(
    status_rows: list[dict], processed: set[int], waiting: set[int], excluded_tenants: list[str]
) -> list[dict]:
    """What `release_cases` does per tenant: `{tenant, case_ids, processed_case_ids}`.

    `case_ids` is every `processing` case whose pipelines are all done and that has variant
    data. `waiting` is discovery run again after the pipelines: a case still in it has a pipeline
    left to run (one failed, or the import has not picked up its new files), so it stays
    `processing` even with variants in -- CNVs imported while its SNV annotation failed must not
    open it for analysis. Every `processing` case is looked at, not only this run's, so the
    later run that finishes it moves it on.

    `processed_case_ids` is the cases past `processing` (`in_progress`, `in_review`...) that this
    run had work for (`processed`, the first discovery) and finished: their status stays, but
    their laboratories are emailed the new results all the same. A `submitted` or `processing`
    case is emailed when it reaches `in_progress` instead.
    """
    cases = [CaseStatus(**row) for row in status_rows if row["status_code"] == PROCESSING]
    finished = [
        CaseStatus(**row)
        for row in status_rows
        if row["status_code"] not in (SUBMITTED, PROCESSING)
        and row["case_id"] in processed
        and row["case_id"] not in waiting
    ]
    held = sorted(c.case_id for c in cases if c.case_id in waiting)
    if held:
        LOGGER.info(
            "%d processing case(s) kept in processing, a pipeline still has work for them: %s", len(held), held
        )
    no_variants = sorted(c.case_id for c in cases if c.case_id not in waiting and not c.has_variants)
    if no_variants:
        LOGGER.warning(
            "%d processing case(s) with no pipeline left to run but no variant data: %s", len(no_variants), no_variants
        )
    ready = _by_tenant(
        [c for c in cases if c.case_id not in waiting and c.has_variants],
        excluded_tenants,
        f"to move to {IN_PROGRESS}",
    )
    notified = _by_tenant(finished, excluded_tenants, "to notify, status unchanged")
    batches = {batch["tenant"]: {**batch, "processed_case_ids": []} for batch in ready}
    for batch in notified:
        batches.setdefault(batch["tenant"], {"tenant": batch["tenant"], "case_ids": []})
        batches[batch["tenant"]]["processed_case_ids"] = batch["case_ids"]
    return [batches[tenant] for tenant in sorted(batches)]


def _by_tenant(cases: list[CaseStatus], excluded_tenants: list[str], action: str) -> list[dict]:
    """One `{tenant, case_ids}` per tenant, leaving out the excluded tenants. The import is not
    filtered by tenant, so an excluded tenant's case can still have variants: its status is left
    alone all the same. A tenant that is not excluded but not granted to the service account gets
    a 403 from the portal instead, and `change_tenant_status` skips it."""
    by_tenant: dict[str, list[int]] = {}
    for case in cases:
        by_tenant.setdefault(case.tenant_code, []).append(case.case_id)

    batches = []
    for tenant in sorted(by_tenant):
        case_ids = sorted(set(by_tenant[tenant]))
        if tenant in excluded_tenants:
            LOGGER.info(
                "tenant '%s' is excluded: %d case(s) left out, not %s: %s", tenant, len(case_ids), action, case_ids
            )
            continue
        LOGGER.info("tenant '%s': %d case(s) %s: %s", tenant, len(case_ids), action, case_ids)
        batches.append({"tenant": tenant, "case_ids": case_ids})
    return batches


def cases_now_in(results: list[dict], status_code: str) -> list[int]:
    """The cases the portal reports in `status_code` after the PATCH, whether this call moved
    them (`updated: true`) or an earlier attempt of the same task did (`updated: false`, already
    there). A retried task must still notify the cases its first attempt moved."""
    return sorted({r["case_id"] for r in results if r.get("current_status_code") == status_code})


def change_tenant_status(batch: dict, status_code: str) -> list[dict]:
    """Send one tenant's status changes; a tenant the service account is not granted on is
    logged and skipped rather than failed.

    The portal refuses the whole batch with a 403 when the service account is missing
    `can_ingest_data` at every lab of the tenant, and nothing is changed. Failing would turn
    the control DAG red every day for a grant nobody asked for; a skip with the reason in the
    log says the same thing without the noise. Any other error fails the task.
    """
    from radiant.tasks.nextflow.register import update_case_system_status

    if not batch["case_ids"]:
        # A tenant with only cases to notify: nothing to move, no PATCH.
        LOGGER.info("tenant '%s': no case to move to %s", batch["tenant"], status_code)
        return []
    return skip_if_not_granted(batch["tenant"], update_case_system_status, batch["case_ids"], status_code)


def skip_if_not_granted(tenant: str, call: Callable, *args):
    """`call(tenant, *args)`, a 403 from the portal turned into a skip of the task."""
    from airflow.exceptions import AirflowSkipException

    from radiant.tasks.nextflow.portal import PortalError

    try:
        return call(tenant, *args)
    except PortalError as error:
        if error.status != 403:
            raise
        LOGGER.warning("tenant '%s' skipped: %s", tenant, error)
        raise AirflowSkipException(f"tenant '{tenant}' is not granted to the service account") from None


def not_ready_cases(state_rows: list[dict]) -> set[int]:
    """The cases one of whose `READINESS_PIPELINES` items is still running (last row `run`, no outcome yet) or
    `failed`, from `pipeline_state`. `wait`, `blocked` and `skip` do not hold a case back."""
    pipelines = {str(p) for p in READINESS_PIPELINES}
    return {
        row["case_id"]
        for row in state_rows
        if row["pipeline"] in pipelines and row["decision"] in (Decision.RUN, Decision.FAILED)
    }


def decide_run(pipeline: Pipeline, fact_rows: list[dict], excluded_tenants: list[str], named: list[int] = ()) -> dict:
    """What a pipeline DAG decides at its start, from its facts: every candidate's decision, and the items to run.

    `named` (cases, or calling tasks for somatic SNV) limits what runs, not what is decided: an item decided `run`
    but not named is left out of the journal, so it does not wait for an outcome this run never writes; the next run
    decides it again. `processing` lists, per tenant, the cases to move to `processing`.
    """
    by_task = pipeline == Pipeline.SOMATIC_SNV
    entries = DECIDE[pipeline](fact_rows, list(excluded_tenants))
    named = set(named)

    def runs(entry: Entry) -> bool:
        key = int(entry.item_key) if by_task else entry.case_id
        return entry.decision == Decision.RUN and (not named or key in named)

    journaled = [e for e in entries if e.decision != Decision.RUN or runs(e)]
    run = [e for e in journaled if e.decision == Decision.RUN]
    tenant_of = {row["case_id"]: row["tenant_code"] for row in fact_rows}
    by_tenant: dict[str, set[int]] = {}
    for entry in run:
        by_tenant.setdefault(tenant_of[entry.case_id], set()).add(entry.case_id)
    LOGGER.info(
        "%s: %d item(s) decided, %d to run%s",
        pipeline,
        len(entries),
        len(run),
        f" (named: {sorted(named)})" if named else "",
    )
    return {
        "entries": [e.model_dump(mode="json") for e in journaled],
        "run_case_ids": sorted({e.case_id for e in run}),
        "run_task_ids": sorted({int(e.item_key) for e in run}) if by_task else [],
        "processing": [
            {"tenant": tenant, "case_ids": sorted(case_ids)}
            for tenant, case_ids in sorted(by_tenant.items())
            if tenant not in excluded_tenants
        ],
    }
