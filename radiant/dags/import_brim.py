import datetime

from airflow.decorators import dag
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

from radiant.tasks.starrocks.operator import (
    RadiantStarrocksLoadOperator,
)

dag_params = {
    "brim_filepaths": Param(
        default=None,
        description="Brim filepaths to load into the brim table.",
        type=["array", "null"],
    ),
}


@dag(
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    params=dag_params,
    tags=["radiant", "scheduled"],
    dag_display_name="Radiant - Import BRIM",
    dag_id="radiant-import-brim",
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def import_brim():
    start = EmptyOperator(task_id="start", task_display_name="[Start]")

    load_brim_table = RadiantStarrocksLoadOperator(
        task_id="load_brim",
        task_display_name="[StarRocks] Load Brim",
        sql="./sql/clinical/brim_load.sql",
        table="{{ mapping.starrocks_brim }}",
        truncate=True,
        load_label="load_brim_{{ ts_nodash }}_{{ ti.try_number }}",
        parameters={"tsv_filepath": "{{ params.brim_filepaths }}"},
    )

    (start >> load_brim_table)


import_brim()
