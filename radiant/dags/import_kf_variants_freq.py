from airflow import DAG
from airflow.operators.empty import EmptyOperator

from radiant.dags import NAMESPACE
from radiant.tasks.starrocks.operator import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

default_args = {
    "owner": "radiant",
}

with DAG(
    dag_id=f"{NAMESPACE}-import-kf-variants-freq",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["radiant", "starrocks"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    drop_if_exists_kf_variants_freq_table = StarRocksSQLExecuteQueryOperator(
        task_id="drop_if_exists",
        sql="DROP TABLE IF EXISTS kf_variants_freq;",
        trigger_rule="all_done",  # Always run, after all upstream are completed
    )

    create_kf_variants_freq_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_table",
        sql="./sql/kf/kf_variants_freq_create_table.sql",
    )

    insert_kf_variants_freq = StarRocksSQLExecuteQueryOperator(
        task_id="insert",
        sql="./sql/kf/kf_variants_freq_insert.sql",
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=7200,
            poll_interval=10,
        ),
    )

    (start >> drop_if_exists_kf_variants_freq_table >> create_kf_variants_freq_table >> insert_kf_variants_freq)
