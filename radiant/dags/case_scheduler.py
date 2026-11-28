"""The single scheduled entry point for case processing: it starts the pipelines, then the import.

It decides nothing and moves no status. Each pipeline DAG decides its own items, journals them, and moves the cases
it runs from `submitted` to `processing` (`radiant.dags.pipeline_decision`); the import does the same for the cases
it loads, then hands them to `radiant-case-release`, which moves the ready ones to `in_progress` and emails the
laboratories.

The pipelines run `all_done`: a failed pipeline must not hold the others back, and the import follows germline SNV,
somatic SNV and germline CNV post-processing (the slivar VCF, RAD-206) whatever their outcome, since it loads what
they registered. `watcher` turns the run red when anything failed, since the `all_done` steps would otherwise let it
end green.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from radiant.dags import DEFAULT_ARGS, EXCLUDED_TENANTS_ENV, NAMESPACE, excluded_tenants_from_env, load_docs_md
from radiant.tasks.nextflow.paths import sanitize_run_tag

GERMLINE_SNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-snv-postprocessing-cases"
GERMLINE_CNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-cnv-postprocessing-cases"
SOMATIC_SNV_CASES_DAG_ID = f"{NAMESPACE}-nextflow-somatic-snv-annotation-cases"
QC_CASES_DAG_ID = f"{NAMESPACE}-nextflow-quality-control-cases"
IMPORT_DAG_ID = f"{NAMESPACE}-import"

# The run's block list, passed to every DAG it starts, so they all leave out the same tenants.
EXCLUDED_TENANTS_CONF = {"excluded_tenants": "{{ params.excluded_tenants }}"}


dag_params = {
    "excluded_tenants": Param(
        excluded_tenants_from_env(),
        type="array",
        items={"type": "string"},
        title="Excluded tenants",
        description=(
            "Tenants left out of case processing: not decided to run by the pipelines, and their case statuses are "
            f"left alone. Defaults to ${EXCLUDED_TENANTS_ENV}; empty excludes none."
        ),
    ),
    "notify": Param(
        True,
        type="boolean",
        title="Notify the laboratories",
        description=(
            "Passed to the import, then to radiant-case-release: email each laboratory the manifest of its cases. "
            "Disable to move them silently; the case group is still created, so the `notify_cases` DAG can send later."
        ),
    ),
}


def _trigger(task_id: str, display_name: str, dag_id: str, conf: dict) -> TriggerDagRunOperator:
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


@dag(
    dag_id=f"{NAMESPACE}-case-scheduler",
    dag_display_name="Radiant - Case Scheduler",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    # A queued run starts after the previous one has finished, and its pipelines decide on what is left.
    max_active_runs=1,
    tags=["radiant", "scheduled", "nextflow"],
    params=dag_params,
    doc_md=load_docs_md("case_scheduler.md"),
    render_template_as_native_obj=True,
    user_defined_macros={"sanitize_run_tag": sanitize_run_tag},
)
def case_scheduler():
    trigger_germline_snv = _trigger(
        "trigger_germline_snv_postprocessing",
        "[DAG] Nextflow SNV Post-processing (from Cases)",
        GERMLINE_SNV_CASES_DAG_ID,
        EXCLUDED_TENANTS_CONF,
    )
    trigger_germline_cnv = _trigger(
        "trigger_germline_cnv_postprocessing",
        "[DAG] Nextflow CNV Post-processing (from Cases)",
        GERMLINE_CNV_CASES_DAG_ID,
        EXCLUDED_TENANTS_CONF,
    )
    trigger_somatic_snv = _trigger(
        "trigger_somatic_snv_annotation",
        "[DAG] Nextflow Somatic SNV Annotation (from Cases)",
        SOMATIC_SNV_CASES_DAG_ID,
        EXCLUDED_TENANTS_CONF,
    )
    trigger_qc = _trigger(
        "trigger_quality_control",
        "[DAG] Nextflow Quality Control (from Cases)",
        QC_CASES_DAG_ID,
        EXCLUDED_TENANTS_CONF,
    )
    trigger_import = _trigger(
        "trigger_import",
        "[DAG] Scheduled Import",
        IMPORT_DAG_ID,
        EXCLUDED_TENANTS_CONF | {"notify": "{{ params.notify }}"},
    )

    @task(
        task_id="watcher",
        task_display_name="[PyOp] Fail the run if a step failed",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    def watcher() -> None:
        raise AirflowFailException("a step of this run failed, see the failed task(s) and the child runs")

    [trigger_germline_snv, trigger_germline_cnv, trigger_somatic_snv] >> trigger_import
    [trigger_germline_snv, trigger_germline_cnv, trigger_somatic_snv, trigger_qc, trigger_import] >> watcher()


case_scheduler()
