from airflow import DAG
from airflow.decorators import task
from airflow.models import Param, Variable
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from commons.operators.starrocks import (
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

dag_params = {
    "from_part": Param(
        default=0,
        description="The inclusive lower bound index of the part to compute when loading occurrences.",
        type="integer",
    ),
    "to_part": Param(
        default=121,
        description="The inclusive upper bound index of the part to compute when loading occurrences.",
        type="integer",
    ),
}

with DAG(
    dag_id="etl_kf_occurrences",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "kf_data"],
    params=dag_params,
) as dag:

    @task
    def get_parts_to_compute(params) -> list[dict]:
        _from = params.get("from_part")
        _to = params.get("to_part")
        if _from > _to:
            raise ValueError("from_part must be less than or equal to to_part")

        return [{"part": i} for i in range(_from, _to + 1)]

    start = EmptyOperator(
        task_id="start",
    )

    create_kf_occurrences_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_table",
        sql="./sql/kf/kf_occurrences_create_table.sql",
    )

    insert_occurrences = StarRocksSQLExecuteQueryOperator.partial(
        task_id="insert_occurrences",
        sql="./sql/kf/kf_occurrences_insert_part.sql",
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=3600,
            poll_interval=10,
            enable_spill=True,
            spill_mode="auto",
        ),
        pool=Variable.get("STARROCKS_INSERT_POOL_ID"),
        pool_slots=1,
        map_index_template="{{ task.parameters['part'] }}",
    ).expand(query_params=get_parts_to_compute())

    drop_if_exists_kf_variants_freq_table = StarRocksSQLExecuteQueryOperator(
        task_id="drop_if_exists_kf_variants_freq",
        sql="DROP TABLE IF EXISTS kf_variants_freq;",
    )

    create_kf_variants_freq_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_kf_variants_freq_table",
        sql="./sql/kf/kf_variants_freq_create_table.sql",
    )

    insert_kf_variants_freq = StarRocksSQLExecuteQueryOperator(
        task_id="insert_kf_variants_freq",
        sql="./sql/kf/kf_variants_freq_insert.sql",
        submit_task=True,
        submit_task_options=SubmitTaskOptions(
            max_query_timeout=7200,
            poll_interval=10,
            enable_spill=True,
            spill_mode="auto",
        ),
    )

    chain(
        start,
        create_kf_occurrences_table,
        insert_occurrences,
        drop_if_exists_kf_variants_freq_table,
        create_kf_variants_freq_table,
        insert_kf_variants_freq,
    )
