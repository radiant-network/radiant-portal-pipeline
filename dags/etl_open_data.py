from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from commons.operators.starrocks import StarRocksSQLExecuteQueryOperator, SubmitTaskOptions


def group_template(group_id):
    insert_hashes = StarRocksSQLExecuteQueryOperator(
        conn_id="starrocks_conn",
        task_id=f"insert_hashes_{group_id}",
        sql=f"./sql/open_data/{group_id}_insert_hashes.sql",
        database=STARROCKS_DATABASE,
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=True,
            spill_mode="auto",
        ),
    )

    create_table_if_not_exists = StarRocksSQLExecuteQueryOperator(
        conn_id="starrocks_conn",
        task_id=f"create_table_{group_id}",
        sql=f"./sql/open_data/{group_id}_create_table.sql",
        database=STARROCKS_DATABASE,
        submit_task=False,
    )

    insert_table = StarRocksSQLExecuteQueryOperator(
        conn_id="starrocks_conn",
        task_id=f"insert_table_{group_id}",
        sql=f"./sql/open_data/{group_id}_insert.sql",
        database=STARROCKS_DATABASE,
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=True,
            spill_mode="auto",
        ),
    )
    return [insert_hashes, create_table_if_not_exists, insert_table]


with DAG(
    dag_id="etl_open_data",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "open_data"],
) as dag:

    STARROCKS_CONNECTION = Variable.get("STARROCKS_CONNECTION")
    STARROCKS_DATABASE = Variable.get("STARROCKS_DATABASE")

    start = EmptyOperator(task_id="start")

    create_variant_dict = StarRocksSQLExecuteQueryOperator(
        conn_id="starrocks_conn",
        task_id="create_variant_dict_table",
        sql="./sql/variant_dict_create_table.sql",
        database=STARROCKS_DATABASE,
        submit_task=False,
    )

    tasks = [start, create_variant_dict]
    group_ids = ["1000_genomes", "clinvar", "dbnsfp", "gnomad", "spliceai", "topmed_bravo"]
    for group in group_ids:
        with TaskGroup(group_id=f"task_group_{group}"):
            _tasks = group_template(group_id=f"{group}")
            for task in _tasks:
                tasks.append(task)

    chain(*tasks)
