from airflow import DAG
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

from radiant.tasks.starrocks.operator import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

NAMESPACE = "radiant"

default_args = {
    "owner": "ferlab",
}

dag_params = {
    "iceberg_catalog": Param(
        default="iceberg_catalog",
        description="The iceberg catalog to use.",
        type="string",
    ),
    "iceberg_database": Param(
        default="iceberg_database",
        description="The iceberg database to use.",
        type="string",
    ),
}

std_submit_task_opts = SubmitTaskOptions(
    max_query_timeout=3600,
    poll_interval=10,
    enable_spill=True,
    spill_mode="auto",
)

with DAG(
    dag_id=f"{NAMESPACE}-import-kf-hashes",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "kf_data"],
    params=dag_params,
) as dag:
    start = EmptyOperator(task_id="start")

    create_variant_dict = StarRocksSQLExecuteQueryOperator(
        task_id="create_variant_dict_table",
        sql="./sql/variant_dict_create_table.sql",
        submit_task=False,
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
            submit_task=True,
            submit_task_options=std_submit_task_opts,
            params={
                "iceberg_catalog": "{{ params.iceberg_catalog }}",
                "iceberg_database": "{{ params.iceberg_database }}",
            },
        )
        for group in group_ids
    ]

    insert_hashes = StarRocksSQLExecuteQueryOperator(
        task_id="variants",
        sql="./sql/kf/kf_variants_insert_hashes.sql",
        submit_task=True,
        submit_task_options=std_submit_task_opts,
        params={
            "iceberg_catalog": "{{ params.iceberg_catalog }}",
            "iceberg_database": "{{ params.iceberg_database }}",
        },
    )

    chain(start, create_variant_dict, *group_tasks, insert_hashes)
