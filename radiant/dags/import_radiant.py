import datetime
import logging
from collections.abc import Sequence
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from radiant.dags import DEFAULT_ARGS, NAMESPACE
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions

logger = logging.getLogger(__name__)


std_submit_task_opts = SubmitTaskOptions(max_query_timeout=3600, poll_interval=10)


def experiment_delta_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    from radiant.tasks.starrocks.partition import SequencingDeltaInput

    column_names = [desc[0] for desc in descriptions[0]]
    dict_rows = [dict(zip(column_names, row, strict=False)) for row in results[0]]
    delta = [vars(SequencingDeltaInput(**row)) for row in dict_rows]
    return [delta]


def experiment_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    from radiant.tasks.starrocks.partition import SequencingDeltaOutput

    column_names = [desc[0] for desc in descriptions[0]]
    dict_rows = [dict(zip(column_names, row, strict=False)) for row in results[0]]
    delta = [vars(SequencingDeltaOutput(**row)) for row in dict_rows]
    return [delta]


@dag(
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["radiant", "scheduled"],
    dag_display_name="Radiant - Scheduled Import",
    dag_id=f"{NAMESPACE}-import",
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def import_radiant():
    start = EmptyOperator(task_id="start", task_display_name="[Start]")

    with TaskGroup(group_id="partitioner_group") as tg_partition_group:
        fetch_sequencing_experiment_delta = RadiantStarRocksOperator(
            task_id="fetch_sequencing_experiment_delta",
            sql="SELECT * FROM {{ params.starrocks_staging_sequencing_experiment_delta }}",
            task_display_name="[StarRocks] Get Sequencing Experiment Delta",
            output_processor=experiment_delta_output_processor,
            do_xcom_push=True,
        )

        @task.short_circuit(
            task_id="assign_partitions",
            task_display_name="[PyOp] Assign Partitions",
            ignore_downstream_trigger_rules=False,
        )
        def assign_partitions(delta: Any) -> Any:
            from radiant.tasks.starrocks.partition import SequencingExperimentPartitionAssigner

            assigner = SequencingExperimentPartitionAssigner()
            rows = [
                tuple(
                    [
                        row.case_id,
                        row.seq_id,
                        row.task_id,
                        row.part,
                        row.analysis_type,
                        str(row.sample_id),
                        row.patient_id,
                        row.experimental_strategy,
                        row.request_id,
                        row.request_priority,
                        row.vcf_filepath,
                        row.sex,
                        row.family_role,
                        row.affected_status,
                        str(row.created_at),
                        str(row.updated_at),
                        str(row.ingested_at),
                    ]
                )
                for row in assigner.assign_partitions(delta)
            ]
            return ",\n".join(str(r) for r in rows)

        assigned_partitions = assign_partitions(fetch_sequencing_experiment_delta.output)

        @task.short_circuit(
            task_id="check_insert_delta",
            task_display_name="[PyOp] Validate Prepared SQL",
            ignore_downstream_trigger_rules=False,
        )
        def check_insert_delta(prepared_sql: Any) -> Any:
            return prepared_sql

        checked = check_insert_delta(assigned_partitions)

        # TODO: Probably a better way to handle this...
        insert_new_sequencing_experiment = RadiantStarRocksOperator(
            sql="""
            INSERT INTO {{ params.starrocks_sequencing_experiment }} (
                case_id, seq_id, task_id, part, analysis_type, sample_id, patient_id, experimental_strategy,
                request_id, request_priority, vcf_filepath, sex, family_role, affected_status, 
                created_at, updated_at, ingested_at) 
            VALUES """
            + str(checked),
            task_id="insert_sequencing_experiment",
            task_display_name="[StarRocks] Insert Partitioned Sequencing Experiment",
        )

        (fetch_sequencing_experiment_delta >> assigned_partitions >> checked >> insert_new_sequencing_experiment)

    fetch_sequencing_experiment = RadiantStarRocksOperator(
        task_id="fetch_sequencing_experiment",
        sql="./sql/radiant/sequencing_experiment_select.sql",
        task_display_name="[StarRocks] Fetch Sequencing Experiments to process",
        output_processor=experiment_output_processor,
        do_xcom_push=True,
        trigger_rule="none_failed",
    )

    @task.short_circuit(task_id="assign_priority", task_display_name="[PyOp] Compute Priority")
    def assign_priority(sequencing_experiment_to_process: Any) -> Any:
        from radiant.tasks.starrocks.partition import SequencingExperimentPriorityAssigner

        prioritized = SequencingExperimentPriorityAssigner.assign_priorities(sequencing_experiment_to_process)
        return [{"part": part} for part in prioritized]

    priority = assign_priority(fetch_sequencing_experiment.output)

    import_parts = TriggerDagRunOperator.partial(
        task_id="import_part",
        task_display_name="[DAG] Import Parts in priority",
        trigger_dag_id=f"{NAMESPACE}-import-part",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        pool="import_part",
    ).expand(conf=priority)

    (start >> tg_partition_group >> fetch_sequencing_experiment >> import_parts)


import_radiant()
