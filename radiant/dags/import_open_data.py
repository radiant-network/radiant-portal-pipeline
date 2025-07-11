import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from radiant.dags import NAMESPACE
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions

LOGGER = logging.getLogger(__name__)


default_args = {"owner": "radiant"}
variant_group_ids = ["1000_genomes", "clinvar", "dbnsfp", "gnomad", "spliceai", "topmed_bravo"]
gene_group_ids = [
    "gnomad_constraint",
    "omim_gene_panel",
    "hpo_gene_panel",
    "orphanet_gene_panel",
    "ddd_gene_panel",
    "cosmic_gene_panel",
]
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
    for group in variant_group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_hashes_{group}",
                task_display_name=f"{group} Insert Hashes",
                sql=f"./sql/open_data/{group}_insert_hashes.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                trigger_rule="none_failed",
            )
        )
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            )
        )

    for group in gene_group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            )
        )


    # TODO : Import RCV files here in a Python operator
    @task(task_id="load_raw_clinvar_rcv_summary", task_display_name="[PyOp] Load Raw ClinVar RCV Summary")
    def load_raw_clinvar_rcv_summary(filepaths: list[str] | None) -> None:
        if not filepaths:
            LOGGER.warning(...)
            return

        import os
        import time

        import jinja2
        from airflow.hooks.base import BaseHook

        from radiant.dags import DAGS_DIR
        from radiant.tasks.data.radiant_tables import get_radiant_mapping

    load_raw_clinvar_rcv_summary = load_raw_clinvar_rcv_summary(files=files)

    insert_clinvar_rcv_summary = RadiantStarRocksOperator(
        task_id="insert_clinvar_rcv_summary",
        task_display_name="[StarRocks] ClinVar RCV Summary Insert Data",
        sql="./sql/open_data/clinvar_rcv_summary_insert.sql",
        submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
    )

    chain(start, *data_tasks, insert_clinvar_rcv_summary)
