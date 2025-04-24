import functools

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from radiant.tasks.starrocks.operator import StarRocksSQLExecuteQueryOperator

default_args = {
    "owner": "ferlab",
}


def parse_parts(**context):
    parts = context["ti"].xcom_pull(task_ids="fetch_sequencing_experiment_delta", key="return_value")
    if parts is None:
        raise ValueError("No parts found in XCom")
    parts_to_process = {p[0] for p in parts}
    context["ti"].xcom_push(key="parts_to_process", value=list(parts_to_process))
    return parts_to_process


run_dag_operator = functools.partial(
    TriggerDagRunOperator,
    conf={"parts": "{{ task_instance.xcom_pull(task_ids='compute_parts', key='parts_to_process') | list | tojson }}"},
    reset_dag_run=True,
    wait_for_completion=True,
    poke_interval=60,
)


with DAG(
    dag_id="import_kf_e2e",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "kf_data"],
    render_template_as_native_obj=True,
) as dag:
    start = EmptyOperator(task_id="start")

    create_sequencing_experiment_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_sequencing_experiment_table",
        sql="./sql/kf/sequencing_experiment_create_table.sql",
    )

    create_sequencing_experiment_delta_view = StarRocksSQLExecuteQueryOperator(
        task_id="create_sequencing_experiment_view",
        sql="./sql/kf/sequencing_experiment_delta_create_view.sql",
    )

    fetch_sequencing_experiment_delta = StarRocksSQLExecuteQueryOperator(
        task_id="fetch_sequencing_experiment_delta",
        sql="SELECT DISTINCT(part) FROM sequencing_experiment_delta",
        do_xcom_push=True,
    )

    compute_parts = ShortCircuitOperator(
        task_id="compute_parts",
        python_callable=parse_parts,
        ignore_downstream_trigger_rules=True,
    )

    import_occurrences = run_dag_operator(task_id="import_occurrences", trigger_dag_id="import_kf_occurrences")
    import_variants = run_dag_operator(task_id="import_variants", trigger_dag_id="import_kf_variants")
    import_consequences = run_dag_operator(task_id="import_consequences", trigger_dag_id="import_kf_consequences")

    update_sequencing_experiments = StarRocksSQLExecuteQueryOperator(
        task_id="insert_new_sequencing_experiments",
        sql="./sql/kf/sequencing_experiment_insert.sql",
    )

    (
        start
        >> create_sequencing_experiment_table
        >> create_sequencing_experiment_delta_view
        >> fetch_sequencing_experiment_delta
        >> compute_parts
        >> import_occurrences
        >> import_variants
        >> import_consequences
        >> update_sequencing_experiments
    )
