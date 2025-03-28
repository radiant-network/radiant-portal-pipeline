from airflow import DAG
from airflow.operators.empty import EmptyOperator

from lib.operators.starrocks import StarRocksSQLExecuteQueryOperator, SubmitTaskOptions

with DAG(
    dag_id="etl_kf_variants",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "kf_data"],
) as dag:

    create_kf_variants_table = StarRocksSQLExecuteQueryOperator(
        conn_id="starrocks_conn",
        task_id="create_table",
        sql="./sql/kf_variants_create_table.sql",
        database="poc_starrocks",
    )

    insert_into_kf_variants_table = StarRocksSQLExecuteQueryOperator(
        conn_id="starrocks_conn",
        task_id="insert_into_table",
        sql="./sql/kf_variants_insert.sql",
        database="poc_starrocks",
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=30,
            enable_spill=False,
            spill_mode="auto",
        ),
    )

    start = EmptyOperator(
        task_id="start",
    )

    start >> create_kf_variants_table
    create_kf_variants_table >> insert_into_kf_variants_table
