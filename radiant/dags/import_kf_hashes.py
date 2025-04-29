from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

from radiant.dags import ICEBERG_COMMON_PARAMS, NAMESPACE
from radiant.tasks.starrocks.operator import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

TASK_PARAMS = {
    "iceberg_catalog": "{{ params.iceberg_catalog }}",
    "iceberg_database": "{{ params.iceberg_database }}",
}

dag_params = ICEBERG_COMMON_PARAMS

std_submit_task_opts = SubmitTaskOptions(max_query_timeout=3600, poll_interval=10)


with DAG(
    dag_id=f"{NAMESPACE}-import-kf-hashes",
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "ferlab"},
    tags=["etl", "kf_data"],
    params=dag_params,
) as dag:
    start = EmptyOperator(task_id="start")

    create_variant_dict = StarRocksSQLExecuteQueryOperator(
        task_id="create_variant_dict_table",
        sql="./sql/variant_dict_create_table.sql",
    )

    group_ids = [
        "1000_genomes",
        "clinvar",
        "dbnsfp",
        "gnomad",
        "spliceai",
        "topmed_bravo",
    ]

    group_tasks = [
        StarRocksSQLExecuteQueryOperator(
            task_id=f"{group}",
            sql=f"./sql/open_data/{group}_insert_hashes.sql",
            submit_task_options=std_submit_task_opts,
            params=TASK_PARAMS,
        )
        for group in group_ids
    ]

    insert_hashes = StarRocksSQLExecuteQueryOperator(
        task_id="variants",
        sql="./sql/kf/kf_variants_insert_hashes.sql",
        submit_task_options=std_submit_task_opts,
        params=TASK_PARAMS,
    )

    chain(start, create_variant_dict, *group_tasks, insert_hashes)
