from airflow import DAG
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from radiant.dags import ICEBERG_COMMON_PARAMS, NAMESPACE
from radiant.tasks.starrocks.operator import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

default_args = {"owner": "ferlab"}

dag_params = {
    **ICEBERG_COMMON_PARAMS,
    "force_import_hashes": Param(
        default=False,
        description="Set to True to force import of hashes into the variant_dict table. (Defaults to False)",
        type="boolean",
    ),
}


def check_import_hashes(**context):
    return context["params"].get("force_import_hashes", False)


def group_template(group_id: str):
    return [
        StarRocksSQLExecuteQueryOperator(
            task_id="create-table",
            sql=f"./sql/open_data/{group_id}_create_table.sql",
            trigger_rule="none_failed",
        ),
        StarRocksSQLExecuteQueryOperator(
            task_id="insert",
            sql=f"./sql/open_data/{group_id}_insert.sql",
            submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            params={
                "iceberg_catalog": "{{ params.iceberg_catalog }}",
                "iceberg_database": "{{ params.iceberg_database }}",
            },
            trigger_rule="none_failed",
        ),
    ]


with DAG(
    dag_id=f"{NAMESPACE}-import-kf-open-data",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "open_data"],
    params=dag_params,
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="hashes") as hashes:
        ShortCircuitOperator(
            task_id="check_skip",
            python_callable=check_import_hashes,
            ignore_downstream_trigger_rules=False,
            trigger_rule="all_done",
        ) >> TriggerDagRunOperator(
            task_id="import_variant_hashes",
            trigger_dag_id=f"{NAMESPACE}-import-kf-hashes",
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=30,
        )

    tasks = [start, hashes]
    group_ids = ["1000_genomes", "clinvar", "dbnsfp", "gnomad", "spliceai", "topmed_bravo"]
    for group in group_ids:
        with TaskGroup(group_id=f"{group}", tooltip=group):
            tasks.extend(group_template(group_id=group))

    chain(*tasks)
