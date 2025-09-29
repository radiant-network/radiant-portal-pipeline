import datetime

from airflow.decorators import dag
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

from radiant.tasks.starrocks.operator import (
    RadiantStarrocksLoadOperator,
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

    refresh_iceberg_tables = RadiantStarrocksLoadOperator(
        task_id="load_test",
        task_display_name="Load Test Table",
        table="{{ mapping.starrocks_raw_clinvar_rcv_summary }}",
        sql="sql/open_data/raw_clinvar_rcv_summary_load.sql",
        truncate=True,
        load_label="load_raw_clinvar_rcv_summary_{{ ts_nodash }}_{{ ti.try_number }}",
        parameters={"rcv_summary_filepaths": "{{ params.raw_rcv_filepaths }}"},
    )

    (start >> refresh_iceberg_tables)


import_test()
