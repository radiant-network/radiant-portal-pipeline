from airflow import DAG
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from commons.operators.starrocks import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)


def group_template(group_id):
    insert_hashes = StarRocksSQLExecuteQueryOperator(
        task_id=f"insert_hashes_{group_id}",
        sql=f"./sql/open_data/{group_id}_insert_hashes.sql",
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=True,
            spill_mode="auto",
        ),
    )

    create_table_if_not_exists = StarRocksSQLExecuteQueryOperator(
        task_id=f"create_table_{group_id}",
        sql=f"./sql/open_data/{group_id}_create_table.sql",
        submit_task=False,
    )

    insert_table = StarRocksSQLExecuteQueryOperator(
        task_id=f"insert_table_{group_id}",
        sql=f"./sql/open_data/{group_id}_insert.sql",
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=True,
            spill_mode="auto",
        ),
    )
    return [insert_hashes, create_table_if_not_exists, insert_table]


dag_params = {
    "starrocks_connection_var_name": Param(
        default="STARROCKS_CONNECTION",
        description="The name of the Airflow variable that contains the StarRocks connection name string.",
        type="string",
    ),
    "starrocks_db_var_name": Param(
        default="STARROCKS_DATABASE",
        description="The name of the Airflow variable that contains the StarRocks database name string.",
        type="string",
    ),
}


with DAG(
    dag_id="etl_open_data",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "open_data"],
    params=dag_params,
) as dag:

    start = EmptyOperator(task_id="start")

    create_variant_dict = StarRocksSQLExecuteQueryOperator(
        task_id="create_variant_dict_table",
        sql="./sql/variant_dict_create_table.sql",
        submit_task=False,
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
        with TaskGroup(group_id=f"task_group_{group}", tooltip=group):
            _tasks = group_template(group_id=group)
            for task in _tasks:
                tasks.append(task)

    chain(*tasks)
