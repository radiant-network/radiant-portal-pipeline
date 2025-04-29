from airflow import DAG
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from radiant.tasks.starrocks.operator import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

NAMESPACE = "radiant"

default_args = {
    "owner": "ferlab",
}

dag_params = {
    "iceberg_catalog": Param(
        default="iceberg_catalog",
        description="The iceberg catalog to use.",
        type="string",
    ),
    "iceberg_database": Param(
        default="iceberg_database",
        description="The iceberg database to use.",
        type="string",
    ),
    "force_import_hashes": Param(
        default=False,
        description="Set to True to force import of hashes into the variant_dict table. (Defaults to False)",
        type="boolean",
    ),
}


def check_import_hashes(**context):
    import_hashes = context["params"].get("force_import_hashes", False)
    return import_hashes


def group_template(group_id: str):
    create_table_if_not_exists = StarRocksSQLExecuteQueryOperator(
        task_id="create-table",
        sql=f"./sql/open_data/{group_id}_create_table.sql",
        submit_task=False,
        trigger_rule="none_failed",
    )

    insert_table = StarRocksSQLExecuteQueryOperator(
        task_id="insert",
        sql=f"./sql/open_data/{group_id}_insert.sql",
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=True,
            spill_mode="auto",
        ),
        params={
            "iceberg_catalog": "{{ params.iceberg_catalog }}",
            "iceberg_database": "{{ params.iceberg_database }}",
        },
        trigger_rule="none_failed",
    )
    return [create_table_if_not_exists, insert_table]


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
        check_import_hashes = ShortCircuitOperator(
            task_id="check_skip",
            python_callable=check_import_hashes,
            ignore_downstream_trigger_rules=False,
            trigger_rule="all_done",
        )

        import_hashes = TriggerDagRunOperator(
            task_id="import_variant_hashes",
            trigger_dag_id=f"{NAMESPACE}-import-kf-hashes",
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=30,
        )

        check_import_hashes >> import_hashes

    tasks = [start, hashes]
    group_ids = [
        "1000_genomes",
        "clinvar",
        "dbnsfp",
        "gnomad",
        "spliceai",
        "topmed_bravo",
    ]
    for group in group_ids:
        with TaskGroup(group_id=f"{group}", tooltip=group):
            _tasks = group_template(group_id=group)
            for task in _tasks:
                tasks.append(task)

    chain(*tasks)
