"""Manual DAG: email the diagnosis laboratories of a case group.

The pipeline equivalent of QLIN's `etl_notify`: re-send a run's notification, or notify an
ad-hoc set of cases. Every run sends; the portal keeps no record of previous emails.
"""

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md

LOGGER = logging.getLogger(__name__)

CASE_GROUP_PREFIX = "manual"


@dag(
    dag_id=f"{NAMESPACE}-notify-cases",
    dag_display_name="Radiant - Notify Laboratories (case group)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["radiant", "nextflow", "manual"],
    doc_md=load_docs_md("notify_cases.md"),
    render_template_as_native_obj=True,
    params={
        "tenant": Param(
            "",
            type="string",
            title="Tenant",
            description="Tenant code of the case group, for example `qlin`.",
        ),
        "case_group_name": Param(
            "",
            type="string",
            title="Case group name",
            description=(
                "An existing group to notify again (the post-processing DAG names its groups "
                "`postprocessing-<run tag>`), or the name to give the group created from "
                "**case_ids**. Empty means `manual-<run tag>`."
            ),
        ),
        "case_ids": Param(
            [],
            type="array",
            items={"type": "integer"},
            title="Case ids",
            description=(
                "Cases to group before notifying. Creates the group, or overwrites the case list of "
                "an existing one with this name. Leave empty to notify an existing group as is."
            ),
        ),
    },
)
def notify_cases():
    @task(task_id="create_case_group", task_display_name="[PyOp] Create or reuse the case group")
    def create_case_group() -> str:
        from airflow.exceptions import AirflowFailException
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.notify import group_name, post_group

        context = get_current_context()
        params = context["params"]
        tenant = str(params["tenant"]).strip()
        if not tenant:
            raise AirflowFailException("the `tenant` param is required")
        name = str(params["case_group_name"]).strip() or group_name(CASE_GROUP_PREFIX, context["run_id"])
        case_ids = sorted({int(i) for i in params["case_ids"]})
        if case_ids:
            post_group(tenant, name, case_ids)
        else:
            LOGGER.info("no case_ids given: notifying the existing case group '%s' in tenant '%s'", name, tenant)
        return name

    @task(task_id="notify_labs", task_display_name="[PyOp] Email the laboratories their manifest")
    def notify_labs(name: str) -> dict:
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.notify import send_notification

        tenant = str(get_current_context()["params"]["tenant"]).strip()
        return send_notification(tenant, name)

    notify_labs(create_case_group())


notify_cases()
