from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from tasks.starrocks.operator import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

default_args = {
    "owner": "ferlab",
}

def group_template(group_id):
    create_table_if_not_exists = StarRocksSQLExecuteQueryOperator(
        task_id=f"create_table",
        sql=f"./sql/open_data/{group_id}_create_table.sql",
        submit_task=False,
    )

    insert_table = StarRocksSQLExecuteQueryOperator(
        task_id=f"insert",
        sql=f"./sql/open_data/{group_id}_insert.sql",
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=True,
            spill_mode="auto",
        ),
    )
    return [create_table_if_not_exists, insert_table]


with DAG(
    dag_id="import_kf_open_data",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "open_data"],
) as dag:
    start = EmptyOperator(task_id="start")
    create_variant_dict = StarRocksSQLExecuteQueryOperator(
        task_id="create_variant_dict_table",
        sql="./sql/open_data/variant_dict_create_table.sql",
    )

    tasks = [start, create_variant_dict]
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
