"""Move to `in_progress` the named cases that are ready, and email their laboratories.

Triggered by `radiant-import` with the cases it loaded new data for; an operator can name cases too. A case is ready
when none of its germline SNV, germline CNV or somatic SNV items is still running or failed (`pipeline_state`).
Ready and `processing`: moved to `in_progress` and emailed. Ready and past `processing`: emailed, status unchanged.
Not ready: left alone, the import that loads the missing data names it again.
"""

import datetime
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, EXCLUDED_TENANTS_ENV, NAMESPACE, excluded_tenants_from_env, load_docs_md
from radiant.tasks.nextflow.case_status import IN_PROGRESS
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, rows_output_processor

DAG_ID = f"{NAMESPACE}-case-release"

# Case group name prefix: `results-<run tag>`, one group per run and tenant. The labs see it, in the manifest's
# file name.
CASE_GROUP_PREFIX = "results"

# The status PATCH is guarded by the expected status, so a retry can only report `updated: false` for what the first
# attempt already changed.
STATUS_RETRIES = 2
STATUS_RETRY_DELAY = datetime.timedelta(minutes=1)

CASE_IDS_PARAMETERS = {"case_ids": "{{ params.case_ids }}"}

# The last journal row of each item of the named cases.
PIPELINE_STATE_SQL = """
SELECT case_id, pipeline, item_key, decision
FROM {{ mapping.starrocks_pipeline_state }}
WHERE case_id IN %(case_ids)s
"""

dag_params = {
    "case_ids": Param(
        [],
        type="array",
        items={"type": "integer"},
        title="Case ids",
        description="The cases to release: `radiant-import` passes the ones it loaded. Empty releases nothing.",
    ),
    "excluded_tenants": Param(
        excluded_tenants_from_env(),
        type="array",
        items={"type": "string"},
        title="Excluded tenants",
        description=(
            f"Tenants whose statuses are left alone. Defaults to ${EXCLUDED_TENANTS_ENV}; empty excludes none."
        ),
    ),
    "notify": Param(
        True,
        type="boolean",
        title="Notify the laboratories",
        description=(
            "Email each laboratory the manifest of its cases. Disable to move them silently; the case group is still "
            "created, so the `notify_cases` DAG can send later."
        ),
    ),
}


@dag(
    dag_id=DAG_ID,
    dag_display_name="Radiant - Case Release",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["radiant", "scheduled"],
    params=dag_params,
    doc_md=load_docs_md("case_release.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def case_release():
    @task.short_circuit(task_id="has_cases", task_display_name="[PyOp] Skip when no case is named")
    def has_cases() -> bool:
        from airflow.sdk import get_current_context

        return bool(get_current_context()["params"]["case_ids"])

    fetch_case_statuses = RadiantStarRocksOperator(
        task_id="fetch_case_statuses",
        task_display_name="[StarRocks] Fetch the statuses of the named cases",
        sql="./sql/clinical/case_status_select.sql",
        parameters=CASE_IDS_PARAMETERS,
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    fetch_pipeline_state = RadiantStarRocksOperator(
        task_id="fetch_pipeline_state",
        task_display_name="[StarRocks] Fetch where the items of the named cases stand",
        sql=PIPELINE_STATE_SQL,
        parameters=CASE_IDS_PARAMETERS,
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="select_ready_cases", task_display_name="[PyOp] Select the ready cases, per tenant")
    def select_ready_cases(status_rows: Any, state_rows: Any) -> Any:
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.case_status import not_ready_cases, plan_in_progress

        params = get_current_context()["params"]
        return plan_in_progress(
            status_rows, set(params["case_ids"]), not_ready_cases(state_rows), params["excluded_tenants"]
        )

    @task(
        task_id="set_in_progress",
        task_display_name="[PyOp] Move cases to in_progress",
        retries=STATUS_RETRIES,
        retry_delay=STATUS_RETRY_DELAY,
    )
    def set_in_progress(batch: dict) -> Any:
        from radiant.tasks.nextflow.case_status import change_tenant_status

        return change_tenant_status(batch, IN_PROGRESS)

    @task(task_id="create_case_group", task_display_name="[PyOp] Group the cases to notify")
    def create_case_group(batch: dict, results: Any) -> str:
        """One group per run and tenant, named after the run tag so a retry overwrites its own."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.case_status import cases_now_in, skip_if_not_granted
        from radiant.tasks.nextflow.notify import group_name, post_group

        case_ids = sorted({*cases_now_in(results, IN_PROGRESS), *batch["processed_case_ids"]})
        if not case_ids:
            raise AirflowSkipException(f"no case of tenant '{batch['tenant']}' to notify")
        name = group_name(CASE_GROUP_PREFIX, get_current_context()["run_id"])
        skip_if_not_granted(batch["tenant"], post_group, name, case_ids)
        return name

    @task(task_id="notify_labs", task_display_name="[PyOp] Email the laboratories their manifest")
    def notify_labs(batch: dict, name: str) -> Any:
        """Stateless on the portal side: a retry of this instance emails the tenant's labs again."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.notify import send_notification

        if not get_current_context()["params"]["notify"]:
            raise AirflowSkipException(f"notify=false: case group '{name}' exists, use the notify_cases DAG to send")
        return send_notification(batch["tenant"], name)

    @task_group(group_id="release_cases", tooltip="Move a tenant's cases to in_progress, then email its labs")
    def release_cases(batch: dict) -> None:
        # One chain per tenant, mapped as a group: a tenant skipped on a 403, or failed, must not hold back the other
        # tenants' emails, as mapping each task on its own would.
        name = create_case_group(batch, set_in_progress(batch))
        notify_labs(batch, name)

    has_cases() >> [fetch_case_statuses, fetch_pipeline_state]
    release_cases.expand(batch=select_ready_cases(fetch_case_statuses.output, fetch_pipeline_state.output))


case_release()
