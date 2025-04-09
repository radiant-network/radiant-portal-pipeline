from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from commons.operators.starrocks import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

with DAG(
    dag_id="etl_kf_variants",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "kf_data"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    tasks = [
        start,
        StarRocksSQLExecuteQueryOperator(
            task_id="create_variant_dict_table",
            sql="./sql/variant_dict_create_table.sql",
        ),
        StarRocksSQLExecuteQueryOperator(
            task_id="insert_kf_variants_hashes",
            sql="./sql/kf/kf_variants_insert_hashes.sql",
            submit_task=True,
            submit_task_options=SubmitTaskOptions(
                max_query_timeout=3600,
                poll_interval=30,
                enable_spill=False,
                spill_mode="auto",
            ),
        ),
        StarRocksSQLExecuteQueryOperator(
            task_id="create_stg_kf_variants_table",
            sql="./sql/kf/stg_kf_variants_create_table.sql",
        ),
        StarRocksSQLExecuteQueryOperator(
            task_id="insert_into_stg_kf_variants",
            sql="./sql/kf/stg_kf_variants_insert.sql",
            submit_task=True,
            submit_task_options=SubmitTaskOptions(
                max_query_timeout=3600,
                poll_interval=30,
                enable_spill=False,
                spill_mode="auto",
            ),
        ),
        StarRocksSQLExecuteQueryOperator(
            task_id="create_kf_variants_table",
            sql="./sql/kf/kf_variants_create_table.sql",
        ),
        StarRocksSQLExecuteQueryOperator(
            task_id="insert_into_kf_variants_table",
            sql="./sql/kf/kf_variants_insert.sql",
            submit_task=True,
            submit_task_options=SubmitTaskOptions(
                max_query_timeout=3600,
                poll_interval=30,
                enable_spill=False,
                spill_mode="auto",
            ),
        ),
    ]

    chain(*tasks)
