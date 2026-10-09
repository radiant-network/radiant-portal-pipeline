"""The single scheduled entry point for case processing, and the only place case statuses change.

Moves the cases with work waiting from `submitted` to `processing`, runs the three "from Cases"
DAGs and the scheduled import, then moves to `in_progress` the `processing` cases that every
pipeline is done with and that have variant data. Those are the only two changes it ever sends.

Everything after `set_processing` runs `all_done`. On a day with no `submitted` case,
`set_processing` is skipped, and the cases already in `processing` or later with new files must
still be processed; a portal outage must not hold the data back either. `watcher` turns the run
red when anything upstream failed, since the `all_done` chain would otherwise end green.

`evaluate_cases` takes every `processing` case, not only this run's, and holds back any case
discovery still finds after the pipelines. A case whose pipeline failed stays `processing`, even
with some variants in, and is moved by the later run that finishes it.

The laboratories are emailed when their cases reach `in_progress` (`evaluate_cases`, one chain per
tenant: `set_in_progress` -> `create_case_group` -> `notify_labs`), so the manifest holds every
pipeline's results. The "from Cases" DAGs no longer email.
"""

import datetime
import logging
import os
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md
from radiant.tasks.nextflow.case_status import (
    CNV_TENANTS_ENVS,
    IN_PROGRESS,
    PROCESSING,
    QC_TENANTS_ENVS,
    SNV_TENANTS_ENVS,
    tenants_from_env,
)
from radiant.tasks.nextflow.paths import sanitize_run_tag
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator

LOGGER = logging.getLogger(__name__)

SNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-snv-postprocessing-cases"
CNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-cnv-postprocessing-cases"
QC_CASES_DAG_ID = f"{NAMESPACE}-nextflow-quality-control-cases"
IMPORT_DAG_ID = f"{NAMESPACE}-import"

# Case group name prefix: `results-<run tag>`, one group per run and tenant. The labs see it,
# in the manifest's file name.
CASE_GROUP_PREFIX = "results"

# The QC DAG's workspace root: its DRAGEN metrics probe only accepts directories under it.
INPUTS_ROOT_ENV = "NEXTFLOW_INPUTS_ROOT"

# The status changes need `can_ingest_data` at every lab ('*') of the tenant: the same service
# account, so the same allow-list as post-processing.
STATUS_TENANTS_ENVS = ("NEXTFLOW_POSTPROCESSING_TENANTS",)

# The status PATCH is guarded by the expected status, so a retry can only report
# `updated: false` for what the first attempt already changed.
STATUS_RETRIES = 2
STATUS_RETRY_DELAY = datetime.timedelta(minutes=1)


def rows_output_processor(results: list[Any], descriptions: list[Any]) -> list[Any]:
    """Cursor rows to a list of dicts."""
    column_names = [desc[0] for desc in descriptions[0]]
    return [[dict(zip(column_names, row, strict=False)) for row in results[0]]]


dag_params = {
    # Not `tenants`: the discovery queries read `params.tenants` as the allow-list of each
    # "from Cases" DAG, set per task below, and a run conf key of that name would override them.
    "status_tenants": Param(
        tenants_from_env(*STATUS_TENANTS_ENVS),
        type="array",
        items={"type": "string"},
        title="Tenant allow-list for status changes",
        description=(
            "Tenants the portal has granted this service account `can_ingest_data` on, at every "
            "laboratory. Cases of any other tenant keep their status, and the tenant is logged. "
            f"Defaults to ${STATUS_TENANTS_ENVS[0]}; empty means no filtering."
        ),
    ),
    "notify": Param(
        True,
        type="boolean",
        title="Notify the laboratories",
        description=(
            "Email each diagnosis laboratory the manifest of its cases moved to `in_progress`. "
            "Disable to move them silently; the case group is still created, so the `notify_cases` "
            "DAG can send later."
        ),
    ),
}


def _trigger(task_id: str, display_name: str, dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        task_display_name=display_name,
        trigger_dag_id=dag_id,
        trigger_run_id="{{ sanitize_run_tag(run_id) }}",
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=True,
        poke_interval=60,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def _discovery(prefix: str, trigger_rule: TriggerRule) -> list[RadiantStarRocksOperator]:
    """The four "work waiting" queries, `{prefix}_{snv,cnv,qc,import}_cases`.

    Built once before the pipelines (`discover`) and once after (`rediscover`), so the question
    asked both times cannot drift. Each "from Cases" DAG's own query, with the allow-list it
    will use: its run passes no conf, so it falls back on the same environment.
    """

    def query(name: str, display_name: str, sql: str, tenants_envs: tuple[str, ...] | None = None):
        extra = {}
        if tenants_envs is not None:
            extra = {
                "params": {"task_ids": [], "tenants": tenants_from_env(*tenants_envs)},
                "parameters": {"tenants": "{{ params.tenants }}"},
            }
        return RadiantStarRocksOperator(
            task_id=f"{prefix}_{name}_cases",
            task_display_name=f"[StarRocks] {display_name}",
            sql=sql,
            output_processor=rows_output_processor,
            do_xcom_push=True,
            trigger_rule=trigger_rule,
            **extra,
        )

    return [
        query(
            "snv",
            "Cases pending SNV post-processing",
            "./sql/clinical/pending_annotation_select.sql",
            SNV_TENANTS_ENVS,
        ),
        query(
            "cnv",
            "Cases pending CNV post-processing",
            "./sql/clinical/pending_cnv_annotation_select.sql",
            CNV_TENANTS_ENVS,
        ),
        query(
            "qc",
            "Cases pending quality control",
            "./sql/clinical/pending_quality_control_select.sql",
            QC_TENANTS_ENVS,
        ),
        query(
            "import",
            "Cases in the import delta",
            "SELECT DISTINCT case_id FROM {{ mapping.starrocks_staging_sequencing_experiment_delta }}",
        ),
    ]


def _cases_with_work(snv_rows: Any, cnv_rows: Any, qc_rows: Any, import_rows: Any) -> set[int]:
    from radiant.tasks.nextflow.case_status import cases_with_work, s3_qc_locator

    def locate_qc(cases):
        # Same probe and same root as the QC DAG's `locate_metrics`, read only when there is a
        # QC candidate: the QC DAG fails the same way when it is unset.
        inputs_root = os.getenv(INPUTS_ROOT_ENV)
        if not inputs_root:
            raise ValueError(f"{INPUTS_ROOT_ENV} must be set to an s3:// uri to locate the DRAGEN metrics")
        return s3_qc_locator(inputs_root)(cases)

    return cases_with_work(snv_rows, cnv_rows, qc_rows, import_rows, locate_qc)


@dag(
    dag_id=f"{NAMESPACE}-case-status-control",
    dag_display_name="Radiant - Case Status Control",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    # Discovery runs at the start of a run, so a queued run re-queries after the previous one
    # has finished and sees what is left.
    max_active_runs=1,
    tags=["radiant", "scheduled", "nextflow"],
    params=dag_params,
    doc_md=load_docs_md("case_status_control.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
    user_defined_macros={"sanitize_run_tag": sanitize_run_tag},
)
def case_status_control():
    discovered = _discovery("discover", TriggerRule.ALL_SUCCESS)
    fetch_case_statuses = RadiantStarRocksOperator(
        task_id="fetch_case_statuses",
        task_display_name="[StarRocks] Fetch submitted and processing cases",
        sql="./sql/clinical/case_status_select.sql",
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="discover_cases", task_display_name="[PyOp] Select the submitted cases with work waiting")
    def discover_cases(snv_rows: Any, cnv_rows: Any, qc_rows: Any, import_rows: Any, status_rows: Any) -> Any:
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.case_status import plan_processing

        waiting = _cases_with_work(snv_rows, cnv_rows, qc_rows, import_rows)
        return plan_processing(status_rows, waiting, get_current_context()["params"]["status_tenants"])

    @task(
        task_id="set_processing",
        task_display_name="[PyOp] Move cases to processing",
        retries=STATUS_RETRIES,
        retry_delay=STATUS_RETRY_DELAY,
    )
    def set_processing(batch: dict) -> Any:
        from radiant.tasks.nextflow.case_status import change_tenant_status

        return change_tenant_status(batch, PROCESSING)

    refresh_case_statuses = RadiantStarRocksOperator(
        task_id="refresh_case_statuses",
        task_display_name="[StarRocks] Fetch processing cases and their variants",
        sql="./sql/clinical/case_status_select.sql",
        output_processor=rows_output_processor,
        do_xcom_push=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(
        task_id="select_ready_cases",
        task_display_name="[PyOp] Select the processing cases that are done and have variants",
    )
    def select_ready_cases(snv_rows: Any, cnv_rows: Any, qc_rows: Any, import_rows: Any, status_rows: Any) -> Any:
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.case_status import plan_in_progress

        still_waiting = _cases_with_work(snv_rows, cnv_rows, qc_rows, import_rows)
        return plan_in_progress(status_rows, still_waiting, get_current_context()["params"]["status_tenants"])

    @task(
        task_id="set_in_progress",
        task_display_name="[PyOp] Move cases to in_progress",
        retries=STATUS_RETRIES,
        retry_delay=STATUS_RETRY_DELAY,
    )
    def set_in_progress(batch: dict) -> Any:
        from radiant.tasks.nextflow.case_status import change_tenant_status

        return change_tenant_status(batch, IN_PROGRESS)

    @task(task_id="create_case_group", task_display_name="[PyOp] Group the cases moved to in_progress")
    def create_case_group(batch: dict, results: Any) -> str:
        """One group per run and tenant, named after the run tag so a retry overwrites its own."""
        from airflow.exceptions import AirflowSkipException
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.case_status import cases_now_in
        from radiant.tasks.nextflow.notify import group_name, post_group

        case_ids = cases_now_in(results, IN_PROGRESS)
        if not case_ids:
            raise AirflowSkipException(f"no case of tenant '{batch['tenant']}' reached {IN_PROGRESS}")
        name = group_name(CASE_GROUP_PREFIX, get_current_context()["run_id"])
        post_group(batch["tenant"], name, case_ids)
        return name

    @task(task_id="notify_labs", task_display_name="[PyOp] Email the laboratories their manifest")
    def notify_labs(batch: dict, name: str) -> Any:
        """Stateless on the portal side: a retry of this instance emails the tenant's labs again."""
        from airflow.exceptions import AirflowSkipException
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.notify import send_notification

        if not get_current_context()["params"]["notify"]:
            raise AirflowSkipException(f"notify=false: case group '{name}' exists, use the notify_cases DAG to send")
        return send_notification(batch["tenant"], name)

    @task_group(group_id="evaluate_cases", tooltip="Move a tenant's cases to in_progress, then email its labs")
    def evaluate_cases(batch: dict) -> None:
        # One chain per tenant, mapped as a group: a tenant skipped on a 403, or failed, must not
        # hold back the other tenants' emails, as mapping each task on its own would.
        name = create_case_group(batch, set_in_progress(batch))
        notify_labs(batch, name)

    @task(
        task_id="watcher",
        task_display_name="[PyOp] Fail the run if a step failed",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    def watcher() -> None:
        raise AirflowFailException("a step of this run failed, see the failed task(s) and the child runs")

    batches = discover_cases(*(query.output for query in discovered), fetch_case_statuses.output)
    processing = set_processing.expand(batch=batches)

    trigger_snv = _trigger(
        "trigger_snv_postprocessing", "[DAG] Nextflow SNV Post-processing (from Cases)", SNV_CASES_DAG_ID
    )
    trigger_cnv = _trigger(
        "trigger_cnv_postprocessing", "[DAG] Nextflow CNV Post-processing (from Cases)", CNV_CASES_DAG_ID
    )
    trigger_qc = _trigger("trigger_quality_control", "[DAG] Nextflow Quality Control (from Cases)", QC_CASES_DAG_ID)
    # The import picks up what post-processing registered, so it waits for it, success or not.
    trigger_import = _trigger("trigger_import", "[DAG] Scheduled Import", IMPORT_DAG_ID)

    processing >> [trigger_snv, trigger_cnv, trigger_qc]
    trigger_snv >> trigger_import
    # Asked again once every pipeline has finished, whatever its outcome: a case still pending
    # somewhere has a pipeline left to run, and stays `processing`.
    rediscovered = _discovery("rediscover", TriggerRule.ALL_DONE)
    pipelines = [trigger_import, trigger_cnv, trigger_qc]
    for query in [*rediscovered, refresh_case_statuses]:
        pipelines >> query

    ready = select_ready_cases(*(query.output for query in rediscovered), refresh_case_statuses.output)
    evaluated = evaluate_cases.expand(batch=ready)

    [
        *discovered,
        fetch_case_statuses,
        processing,
        trigger_snv,
        trigger_cnv,
        trigger_qc,
        trigger_import,
        *rediscovered,
        refresh_case_statuses,
        evaluated,
    ] >> watcher()


case_status_control()
