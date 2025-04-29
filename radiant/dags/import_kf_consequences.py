from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from radiant.tasks.starrocks.operator import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

default_args = {
    "owner": "ferlab",
}

std_submit_task_opts = SubmitTaskOptions(
    max_query_timeout=3600,
    poll_interval=30,
    enable_spill=True,
    spill_mode="auto",
)

dag_params = {
    "parts": Param(
        default=None,
        description="An array of integers that represents the parts that need to be processed. ",
        type="array",
    ),
}

with DAG(
    dag_id="import_kf_consequences",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "kf_data"],
    params=dag_params,
) as dag:
    start = EmptyOperator(task_id="start")

    create_kf_consequences_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_table",
        sql="./sql/kf/kf_consequences_create_table.sql",
    )

    insert_into_kf_consequences = StarRocksSQLExecuteQueryOperator(
        task_id="insert",
        sql="./sql/kf/kf_consequences_insert.sql",
        submit_task=True,
        submit_task_options=std_submit_task_opts,
    )

    create_kf_consequences_filter_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_filter_table",
        sql="./sql/kf/kf_consequences_filter_create_table.sql",
    )

    fetch_filter_partitions = StarRocksSQLExecuteQueryOperator(
        task_id="fetch_filter_partitions",
        sql="SELECT part FROM test_etl.consequences_filter GROUP BY part HAVING count(1) > 0",
        do_xcom_push=True,
        trigger_rule="all_done",
    )

    with TaskGroup(group_id="insert_filter_partitions") as insert_new_partitions:

        @task
        def get_new_parts(consequences_filter_partitions, params) -> list[dict]:
            _ids = set([int(p) for p in params.get("parts")]) - set([p[0] for p in consequences_filter_partitions])
            return [{"part": i} for i in _ids]

        insert_new_kf_consequences_filter_partitions = StarRocksSQLExecuteQueryOperator.partial(
            task_id="insert",
            sql="./sql/kf/kf_consequences_filter_insert_part.sql",
            submit_task=True,
            submit_task_options=std_submit_task_opts,
            pool="starrocks_insert_pool",
            pool_slots=1,
        ).expand(
            query_params=get_new_parts(
                consequences_filter_partitions=fetch_filter_partitions.output,
            )
        )

    with TaskGroup(group_id="overwrite_filter_partitions") as overwrite_partitions:

        @task
        def get_overwrite_parts(consequences_filter_partitions, params) -> list[dict]:
            _ids = set([int(p) for p in params.get("parts")]) & set([p[0] for p in consequences_filter_partitions])
            return [{"part": i} for i in _ids]

        insert_overwrite_kf_variants_partitions = StarRocksSQLExecuteQueryOperator.partial(
            task_id="insert",
            sql="./sql/kf/kf_consequences_filter_overwrite_part.sql",
            submit_task=True,
            submit_task_options=std_submit_task_opts,
            pool="starrocks_insert_pool",
            pool_slots=1,
        ).expand(
            query_params=get_overwrite_parts(
                consequences_filter_partitions=fetch_filter_partitions.output,
            )
        )

    (
        start
        >> create_kf_consequences_table
        >> insert_into_kf_consequences
        >> create_kf_consequences_filter_table
        >> fetch_filter_partitions
        >> [
            insert_new_partitions,
            overwrite_partitions,
        ]
    )
