"""The single scheduled entry point for case processing, and the only place case statuses change.

Moves the cases with work waiting from `submitted` to `processing`, runs the four "from Cases"
DAGs and the scheduled import, then moves to `in_progress` the `processing` cases that every
pipeline is done with and that have variant data. Those are the only two changes it ever sends.

The pipelines run `all_done`. On a day with no `submitted` case, `set_processing` is skipped, and
the cases already in `processing` or later with new files must still be processed; a portal outage
must not hold the data back either. The evaluation after them runs `none_failed`: a failed import,
CNV post-processing or quality control stops the run there, and no status changes nor email go out.
`watcher` turns the run red when anything upstream failed, since the `all_done` steps would
otherwise let it end green.

`evaluate_cases` takes every `processing` case, not only this run's, and holds back any case
discovery still finds after the pipelines. A case whose pipeline failed stays `processing`, even
with some variants in, and is moved by the later run that finishes it.

The laboratories are emailed at the end of the run (`evaluate_cases`, one chain per tenant:
`set_in_progress` -> `create_case_group` -> `notify_labs`), so the manifest holds every pipeline's
results: for the cases moved to `in_progress`, and for the cases already past it that the run
processed and finished, whose status is left alone. The "from Cases" DAGs no longer email.
"""

import datetime
import logging
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from radiant.dags import DEFAULT_ARGS, EXCLUDED_TENANTS_ENV, NAMESPACE, excluded_tenants_from_env, load_docs_md
from radiant.tasks.nextflow.case_status import IN_PROGRESS, PROCESSING
from radiant.tasks.nextflow.paths import sanitize_run_tag

LOGGER = logging.getLogger(__name__)

GERMLINE_SNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-snv-postprocessing-cases"
GERMLINE_CNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-cnv-postprocessing-cases"
SOMATIC_SNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-somatic-snv-annotation-cases"
QC_CASES_DAG_ID = f"{NAMESPACE}-nextflow-quality-control-cases"
IMPORT_DAG_ID = f"{NAMESPACE}-import"

# Case group name prefix: `results-<run tag>`, one group per run and tenant. The labs see it,
# in the manifest's file name.
CASE_GROUP_PREFIX = "results"

# Passed to the four "from Cases" DAGs:
# - the run's block list, so they leave out exactly the tenants discovery left out. A tenant that is
#   not excluded but not granted to the service account gets a 403 from the portal instead, logged
#   and skipped;
# - the run whose decisions they follow: they launch only the units `discover_cases`
#   decided `run`, and a unit that became runnable since waits for the next run.
CHILD_CONF = {
    "excluded_tenants": "{{ params.excluded_tenants }}",
    "decision_run_id": "{{ sanitize_run_tag(run_id) }}",
}

# The status PATCH is guarded by the expected status, so a retry can only report
# `updated: false` for what the first attempt already changed.
STATUS_RETRIES = 2
STATUS_RETRY_DELAY = datetime.timedelta(minutes=1)


dag_params = {
    "excluded_tenants": Param(
        excluded_tenants_from_env(),
        type="array",
        items={"type": "string"},
        title="Excluded tenants",
        description=(
            "Tenants left out of case processing: not discovered, not processed by the four "
            '"from Cases" DAGs, and their case statuses are left alone. Defaults to '
            f"${EXCLUDED_TENANTS_ENV}; empty excludes none."
        ),
    ),
    "notify": Param(
        True,
        type="boolean",
        title="Notify the laboratories",
        description=(
            "Email each diagnosis laboratory the manifest of its cases moved to `in_progress`, "
            "and of its cases past it that the run processed. "
            "Disable to move them silently; the case group is still created, so the `notify_cases` "
            "DAG can send later."
        ),
    ),
}


def _trigger(task_id: str, display_name: str, dag_id: str, conf: dict | None = None) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        task_display_name=display_name,
        trigger_dag_id=dag_id,
        trigger_run_id="{{ sanitize_run_tag(run_id) }}",
        conf=conf,
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=True,
        poke_interval=60,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def _decide_and_log(stage: str, context: Any) -> None:
    """Read every pipeline's facts, decide, and write the decisions to `pipeline_decision_log` under this
    run (every `run`, the others on change). The work waiting is then read back from the log.

    Asked the same way before the pipelines (`discover`) and after them (`after`), so the question
    cannot drift. The facts are the "from Cases" DAGs' own queries; the run's `excluded_tenants` is
    applied in Python, as in those DAGs.
    """
    from radiant.tasks.nextflow.decision import (
        GERMLINE_CNV,
        GERMLINE_SNV,
        IMPORT,
        QC,
        SOMATIC_SNV,
        decide,
        read_discovery,
        write_decisions,
    )

    run_tag = sanitize_run_tag(context["run_id"])
    conf = context["dag_run"].conf
    rows = read_discovery(conf)
    decisions = decide(
        rows[GERMLINE_SNV],
        rows[GERMLINE_CNV],
        rows[SOMATIC_SNV],
        rows[QC],
        rows[IMPORT],
        excluded_tenants=context["params"]["excluded_tenants"],
    )
    write_decisions(decisions, run_tag, stage, conf)


def _case_statuses(context: Any) -> list[dict]:
    from radiant.tasks.nextflow.decision import read_rows

    return read_rows("clinical", "case_status_select.sql", context["dag_run"].conf)


def _waiting(stage: str, context: Any) -> set[int]:
    """The cases with work waiting at `stage` of this run: its `run` decisions in the log."""
    from radiant.tasks.nextflow.decision import read_run_case_ids

    return read_run_case_ids(sanitize_run_tag(context["run_id"]), stage, context["dag_run"].conf)


def _processing_plan(context: Any) -> list[dict]:
    """`{tenant, case_ids}` to move to `processing`, from the log and the current statuses."""
    from radiant.tasks.nextflow.case_status import plan_processing
    from radiant.tasks.nextflow.decision import DISCOVER

    waiting = _waiting(DISCOVER, context)
    return plan_processing(_case_statuses(context), waiting, context["params"]["excluded_tenants"])


def _in_progress_plan(context: Any) -> list[dict]:
    """`{tenant, case_ids, processed_case_ids}` to move to `in_progress` and to notify, from the log
    (the work this run had at `discover`, the work still waiting `after`) and the current statuses.
    A failed discovery leaves no `discover` row: the cases are still moved, only the others are not
    emailed."""
    from radiant.tasks.nextflow.case_status import plan_in_progress
    from radiant.tasks.nextflow.decision import AFTER, DISCOVER

    processed, waiting = _waiting(DISCOVER, context), _waiting(AFTER, context)
    return plan_in_progress(_case_statuses(context), processed, waiting, context["params"]["excluded_tenants"])


def _tenant_batch(plan: list[dict], tenant: str) -> dict:
    return next(
        (b for b in plan if b["tenant"] == tenant), {"tenant": tenant, "case_ids": [], "processed_case_ids": []}
    )


@dag(
    dag_id=f"{NAMESPACE}-case-scheduler",
    dag_display_name="Radiant - Case Scheduler",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    # Discovery runs at the start of a run, so a queued run re-queries after the previous one
    # has finished and sees what is left.
    max_active_runs=1,
    tags=["radiant", "scheduled", "nextflow"],
    params=dag_params,
    doc_md=load_docs_md("case_scheduler.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
    user_defined_macros={"sanitize_run_tag": sanitize_run_tag},
)
def case_scheduler():
    @task(
        task_id="discover_cases",
        task_display_name="[PyOp] Decide every pipeline and select the submitted cases with work waiting",
    )
    def discover_cases() -> list[str]:
        """The tenants with cases to move to `processing`: the only XCom, each tenant's task reads
        its cases back from the log."""
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.decision import DISCOVER

        context = get_current_context()
        _decide_and_log(DISCOVER, context)
        return [batch["tenant"] for batch in _processing_plan(context)]

    @task(
        task_id="set_processing",
        task_display_name="[PyOp] Move cases to processing",
        retries=STATUS_RETRIES,
        retry_delay=STATUS_RETRY_DELAY,
    )
    def set_processing(tenant: str) -> Any:
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.case_status import change_tenant_status

        return change_tenant_status(_tenant_batch(_processing_plan(get_current_context()), tenant), PROCESSING)

    @task(
        task_id="select_ready_cases",
        task_display_name="[PyOp] Decide again and select the cases to move to in_progress and to notify",
        # Asked once the import, CNV post-processing and quality control have succeeded: if any of
        # them failed, nothing is evaluated nor emailed; the next run picks the cases up.
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    def select_ready_cases() -> list[str]:
        """The tenants with cases to move to `in_progress` or to notify."""
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.decision import AFTER

        context = get_current_context()
        _decide_and_log(AFTER, context)
        return [batch["tenant"] for batch in _in_progress_plan(context)]

    @task(
        task_id="set_in_progress",
        task_display_name="[PyOp] Move cases to in_progress",
        retries=STATUS_RETRIES,
        retry_delay=STATUS_RETRY_DELAY,
    )
    def set_in_progress(tenant: str) -> Any:
        """The portal's answer, kept in XCom: which cases did move."""
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.case_status import change_tenant_status

        return change_tenant_status(_tenant_batch(_in_progress_plan(get_current_context()), tenant), IN_PROGRESS)

    @task(task_id="create_case_group", task_display_name="[PyOp] Group the cases to notify")
    def create_case_group(tenant: str, results: Any) -> None:
        """One group per run and tenant, named after the run tag so a retry overwrites its own.

        The cases moved to `in_progress`, and the cases past it this run finished processing (read
        again: the ones just moved are among them too, which changes nothing to the union).
        """
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.case_status import cases_now_in, skip_if_not_granted
        from radiant.tasks.nextflow.notify import group_name, post_group

        context = get_current_context()
        processed = _tenant_batch(_in_progress_plan(context), tenant)["processed_case_ids"]
        case_ids = sorted({*cases_now_in(results, IN_PROGRESS), *processed})
        if not case_ids:
            raise AirflowSkipException(f"no case of tenant '{tenant}' to notify")
        skip_if_not_granted(tenant, post_group, group_name(CASE_GROUP_PREFIX, context["run_id"]), case_ids)

    @task(task_id="notify_labs", task_display_name="[PyOp] Email the laboratories their manifest")
    def notify_labs(tenant: str) -> Any:
        """Stateless on the portal side: a retry of this instance emails the tenant's labs again.
        Skipped with `create_case_group`, when there is no group to send."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.notify import group_name, send_notification

        context = get_current_context()
        name = group_name(CASE_GROUP_PREFIX, context["run_id"])
        if not context["params"]["notify"]:
            raise AirflowSkipException(f"notify=false: case group '{name}' exists, use the notify_cases DAG to send")
        return send_notification(tenant, name)

    @task_group(group_id="evaluate_cases", tooltip="Move a tenant's cases to in_progress, then email its labs")
    def evaluate_cases(tenant: str) -> None:
        # One chain per tenant, mapped as a group: a tenant skipped on a 403, or failed, must not
        # hold back the other tenants' emails, as mapping each task on its own would.
        create_case_group(tenant, set_in_progress(tenant)) >> notify_labs(tenant)

    @task(
        task_id="watcher",
        task_display_name="[PyOp] Fail the run if a step failed",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    def watcher() -> None:
        raise AirflowFailException("a step of this run failed, see the failed task(s) and the child runs")

    batches = discover_cases()
    processing = set_processing.expand(tenant=batches)

    with TaskGroup(
        group_id="pipelines",
        prefix_group_id=False,
        tooltip="The four from Cases DAGs, then the scheduled import",
    ):
        trigger_germline_snv = _trigger(
            "trigger_germline_snv_postprocessing",
            "[DAG] Nextflow SNV Post-processing (from Cases)",
            GERMLINE_SNV_CASES_DAG_ID,
            CHILD_CONF,
        )
        trigger_germline_cnv = _trigger(
            "trigger_germline_cnv_postprocessing",
            "[DAG] Nextflow CNV Post-processing (from Cases)",
            GERMLINE_CNV_CASES_DAG_ID,
            CHILD_CONF,
        )
        trigger_somatic_snv = _trigger(
            "trigger_somatic_snv_annotation",
            "[DAG] Nextflow Somatic SNV Annotation (from Cases)",
            SOMATIC_SNV_CASES_DAG_ID,
            CHILD_CONF,
        )
        trigger_qc = _trigger(
            "trigger_quality_control",
            "[DAG] Nextflow Quality Control (from Cases)",
            QC_CASES_DAG_ID,
            CHILD_CONF,
        )
        # The import picks up the annotations SNV post-processing, somatic annotation and CNV
        # post-processing (the slivar VCF) registered, so it waits for the three, success or not.
        trigger_import = _trigger("trigger_import", "[DAG] Scheduled Import", IMPORT_DAG_ID)
        [trigger_germline_snv, trigger_germline_cnv, trigger_somatic_snv] >> trigger_import
    pipelines = [trigger_germline_snv, trigger_germline_cnv, trigger_somatic_snv, trigger_qc, trigger_import]

    processing >> pipelines[:4]
    ready = select_ready_cases()
    [trigger_import, trigger_germline_cnv, trigger_qc] >> ready
    evaluated = evaluate_cases.expand(tenant=ready)

    [batches, processing, *pipelines, ready, evaluated] >> watcher()


case_scheduler()
