import datetime

from airflow.decorators import dag
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

from radiant.tasks.starrocks.operator import (
    RadiantStarRocksOperator,
)

dag_params = {
    "raw_rcv_filepaths": Param(
        default=None,
        description="RCV filepaths to load into the raw ClinVar RCV Summary table.",
        type=["array", "null"],
    ),
}


@dag(
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    params=dag_params,
    tags=["radiant", "scheduled"],
    dag_display_name="Radiant - ImportTest",
    dag_id="radiant-import-test",
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def import_test():
    start = EmptyOperator(task_id="start", task_display_name="[Start]")

    refresh_iceberg_tables = RadiantStarRocksOperator(
        task_id="fetch_sequencing_experiment_delta",
        sql="""
            SELECT *
            FROM radiant_iceberg_catalog.radiant.germline_cnv_occurrence o
            where seq_id in %(seq_ids)s OR seq_id in %(seq_ids)s limit 10
            """,
        task_display_name="[StarRocks] Get Sequencing Experiment for a partition",
        parameters={"seq_ids": [1, 2, 3, 22]},
    )

    (start >> refresh_iceberg_tables)


import_test()
