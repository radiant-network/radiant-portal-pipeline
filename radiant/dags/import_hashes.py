from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

from radiant.dags import ICEBERG_COMMON_DAG_PARAMS, ICEBERG_COMMON_TASK_PARAMS, NAMESPACE
from radiant.tasks.starrocks.operator import (
    RadiantStarRocksOperator,
    SubmitTaskOptions,
)

default_args = {
    "owner": "radiant",
}

dag_params = ICEBERG_COMMON_DAG_PARAMS

std_submit_task_opts = SubmitTaskOptions(max_query_timeout=3600, poll_interval=10)


with DAG(
    dag_id=f"{NAMESPACE}-import-hashes",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["radiant", "starrocks"],
    params=dag_params,
) as dag:
    start = EmptyOperator(task_id="start")

    create_variant_dict = RadiantStarRocksOperator(
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
        RadiantStarRocksOperator(
            task_id=f"{group}",
            sql=f"./sql/open_data/{group}_insert_hashes.sql",
            submit_task_options=std_submit_task_opts,
            params=ICEBERG_COMMON_TASK_PARAMS,
        )
        for group in group_ids
    ]

    insert_hashes = RadiantStarRocksOperator(
        task_id="variants",
        sql="./sql/kf/kf_variants_insert_hashes.sql",
        submit_task_options=std_submit_task_opts,
        params=ICEBERG_COMMON_TASK_PARAMS,
    )

    chain(start, create_variant_dict, *group_tasks, insert_hashes)
