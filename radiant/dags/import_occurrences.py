from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from radiant.dags import NAMESPACE
from radiant.tasks.starrocks.operator import (
    STARROCKS_INSERT_POOL,
    StarRocksSQLExecuteQueryOperator,
    SubmitTaskOptions,
)

default_args = {
    "owner": "radiant",
}

std_submit_task_opts = SubmitTaskOptions(
    max_query_timeout=3600,
    poll_interval=30,
    enable_spill=True,
    spill_mode="auto",
)

dag_params = {
    "parts": Param(
        default=None,
        description="An array of integers that represents the parts that need to be processed. ",
        type="array",
    ),
    "force_import_stg_variants": Param(
        default=False,
        description="Set to True to force import of stg_variants table. (Defaults to False)",
        type="boolean",
    ),
}


def check_import_stg_variants(**context):
    import_stg_variants = context["params"].get("force_import_stg_variants", False)
    return import_stg_variants


with DAG(
    dag_id=f"{NAMESPACE}-import-occurrences",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["radiant", "starrocks"],
    params=dag_params,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    create_occurrences_table = StarRocksSQLExecuteQueryOperator(
        task_id="create_table",
        sql="./sql/radiant/occurrences_create_table.sql",
    )

    create_occurrences_bitmap_index = StarRocksSQLExecuteQueryOperator(
        task_id="create_bitmap_index",
        sql="CREATE INDEX locus_id_index ON occurrences (locus_id) USING BITMAP;",
    )

    with TaskGroup(group_id="stg_variants") as tg_hashes:
        check_should_skip_stg_variants = ShortCircuitOperator(
            task_id="check_skip",
            python_callable=check_import_stg_variants,
            ignore_downstream_trigger_rules=False,
            trigger_rule="all_done",
        )
        # Note on trigger_rule="all_done":
        # StarRocks doesn't support 'if not exists' for indexes.
        # It's "safe-ish" to continue even if the index creation fails.
        create_stg_variants = StarRocksSQLExecuteQueryOperator(
            task_id="create_table",
            sql="./sql/radiant/stg_variants_create_table.sql",
        )

        insert_stg_variants = StarRocksSQLExecuteQueryOperator(
            task_id="insert",
            sql="./sql/radiant/stg_variants_insert.sql",
            submit_task_options=std_submit_task_opts,
        )
        check_should_skip_stg_variants >> create_stg_variants >> insert_stg_variants

    fetch_partitions = StarRocksSQLExecuteQueryOperator(
        task_id="fetch_occurrences_partitions",
        sql="""
        SELECT part FROM test_etl.occurrences
        WHERE part IN ({{ params.parts | join(',') }})
        GROUP BY part
        HAVING count(1) > 0
        """,
        do_xcom_push=True,
        trigger_rule="all_done",
    )

    with TaskGroup(group_id="insert_occurrences_partitions") as insert_new_occurrences:

        @task
        def get_parts_to_insert(fetch_output, params) -> list[dict]:
            _parts = {int(p) for p in params.get("parts")} - set([p[0] for p in fetch_output])
            return [{"part": i} for i in _parts]

        insert_new_occurrences_partitions = StarRocksSQLExecuteQueryOperator.partial(
            task_id="insert",
            sql="./sql/radiant/occurrences_insert_part.sql",
            submit_task_options=std_submit_task_opts,
            pool=STARROCKS_INSERT_POOL,
            pool_slots=1,
        ).expand(query_params=get_parts_to_insert(fetch_partitions.output))

    with TaskGroup(group_id="overwrite_occurrences_partitions") as overwrite_occurrences:

        @task
        def get_parts_to_overwrite(fetch_output) -> list[dict]:
            return [{"part": i} for i in {p[0] for p in fetch_output}]

        insert_overwrite_occurrences_partitions = StarRocksSQLExecuteQueryOperator.partial(
            task_id="overwrite",
            sql="./sql/radiant/occurrences_overwrite_part.sql",
            submit_task_options=std_submit_task_opts,
            pool=STARROCKS_INSERT_POOL,
            pool_slots=1,
        ).expand(query_params=get_parts_to_overwrite(fetch_partitions.output))

    import_variants_freq = TriggerDagRunOperator(
        task_id="import_variants_freq",
        trigger_dag_id="import_variants_freq",
        conf={"parts": "{{ params.parts | list | tojson }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )

    (
        start
        >> create_occurrences_table
        >> create_occurrences_bitmap_index
        >> tg_hashes
        >> fetch_partitions
        >> [
            insert_new_occurrences,
            overwrite_occurrences,
        ]
        >> import_variants_freq
    )
