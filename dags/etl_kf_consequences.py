from airflow import DAG
from airflow.operators.empty import EmptyOperator
from commons.operators.starrocks import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

with DAG(
    dag_id="etl_kf_consequences",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "kf_data"],
) as dag:
    create_kf_consequences_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_table",
        sql="./sql/kf/kf_consequences_create_table.sql",
    )

    insert_into_kf_consequences = StarRocksSQLExecuteQueryOperator(
        task_id="insert_into",
        sql="./sql/kf/kf_consequences_insert.sql",
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=True,
            spill_mode="auto",
        ),
    )

    start = EmptyOperator(
        task_id="start",
    )

    start >> create_kf_consequences_table
    create_kf_consequences_table >> insert_into_kf_consequences
