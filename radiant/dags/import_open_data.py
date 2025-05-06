from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from radiant.dags import ICEBERG_COMMON_TASK_PARAMS, NAMESPACE
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions

default_args = {"owner": "radiant"}
group_ids = ["1000_genomes", "clinvar", "dbnsfp", "gnomad", "spliceai", "topmed_bravo"]
with DAG(
    dag_id=f"{NAMESPACE}-import-open-data",
    dag_display_name="Radiant - Import Open Data",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["radiant", "starrocks", "open-data", "manual"],
) as dag:
    start = EmptyOperator(task_id="start")

    data_tasks = []
    for group in group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_hashes_{group}",
                task_display_name=f"{group} Insert Hashes",
                sql=f"./sql/open_data/{group}_insert_hashes.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                params=ICEBERG_COMMON_TASK_PARAMS,
                trigger_rule="none_failed",
            )
        )
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                params=ICEBERG_COMMON_TASK_PARAMS,
                trigger_rule="none_failed",
            )
        )
    data_tasks.append(
        RadiantStarRocksOperator(
            task_id="insert_gnomad_constraints",
            task_display_name="gnomad_constraints Insert Data",
            sql="./sql/open_data/gnomad_constraints_insert.sql",
            submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            params=ICEBERG_COMMON_TASK_PARAMS,
            trigger_rule="none_failed",
        )
    )

    chain(start, *data_tasks)
