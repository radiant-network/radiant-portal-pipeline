from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from radiant.dags import NAMESPACE
from radiant.tasks.starrocks.operator import (
    STARROCKS_INSERT_POOL,
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

default_args = {
    "owner": "radiant",
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
    dag_id=f"{NAMESPACE}-import-variants",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["radiant", "starrocks"],
    params=dag_params,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    create_kf_variant_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_table",
        sql="./sql/kf/kf_variants_create_table.sql",
    )

    insert_kf_variant = StarRocksSQLExecuteQueryOperator(
        task_id="insert",
        sql="./sql/kf/kf_variants_insert.sql",
        submit_task_options=std_submit_task_opts,
    )

    create_kf_variant_part_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_partitions_table",
        sql="./sql/kf/kf_variants_part_create_table.sql",
    )

    fetch_partitions = StarRocksSQLExecuteQueryOperator(
        task_id="fetch_variants_partitions",
        sql="""
        SELECT part FROM test_etl.kf_variants_part
        GROUP BY part HAVING count(1) > 0
        """,
        do_xcom_push=True,
        trigger_rule="all_done",
    )

    with TaskGroup(group_id="insert_variants_partitions") as insert_new_partitions:

        @task
        def get_new_parts(variants_partitions, params) -> list[dict]:
            _ids = set([int(p) // 10 for p in params.get("parts")]) - set([p[0] for p in variants_partitions])
            return [{"part_id": i, "part_lower": i * 10, "part_upper": (i * 10) + 10} for i in _ids]

        insert_part = StarRocksSQLExecuteQueryOperator.partial(
            task_id="insert",
            sql="./sql/kf/kf_variants_part_insert_part.sql",
            submit_task_options=std_submit_task_opts,
            pool=STARROCKS_INSERT_POOL,
            pool_slots=1,
        ).expand(
            query_params=get_new_parts(
                variants_partitions=fetch_partitions.output,
            )
        )

    with TaskGroup(group_id="overwrite_variants_partitions") as overwrite_partitions:

        @task
        def get_overwrite_parts(variants_partitions, params) -> list[dict]:
            _ids = set([int(p) // 10 for p in params.get("parts")]) & set([p[0] for p in variants_partitions])
            return [{"part_id": i, "part_lower": i * 10, "part_upper": (i * 10) + 10} for i in _ids]

        overwrite_part = StarRocksSQLExecuteQueryOperator.partial(
            task_id="overwrite",
            sql="./sql/kf/kf_variants_part_overwrite_part.sql",
            submit_task_options=std_submit_task_opts,
            pool=STARROCKS_INSERT_POOL,
            pool_slots=1,
        ).expand(
            query_params=get_overwrite_parts(
                variants_partitions=fetch_partitions.output,
            )
        )

    (
        start
        >> create_kf_variant_table
        >> insert_kf_variant
        >> create_kf_variant_part_table
        >> fetch_partitions
        >> [
            insert_new_partitions,
            overwrite_partitions,
        ]
    )
