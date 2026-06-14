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
            sql="SELECT * FROM {{ mapping.starrocks_staging_sequencing_experiment_delta }}",
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
            return [row.__dict__ for row in assigner.assign_partitions(delta)]

        assigned_partitions = assign_partitions(fetch_sequencing_experiment_delta.output)

        @task(
            task_id="insert_sequencing_experiment",
            task_display_name="[PyOp] Insert New Sequencing Experiments",
            retries=2,
            retry_delay=datetime.timedelta(seconds=30),
        )
        def insert_new_sequencing_experiment(sequencing_experiment: Any):
            import os

            import jinja2
            from airflow.hooks.base import BaseHook
            from airflow.operators.python import get_current_context

            from radiant.dags import DAGS_DIR
            from radiant.tasks.data.radiant_tables import get_radiant_mapping

            context = get_current_context()
            dag_conf = context["dag_run"].conf or {}
            _path = os.path.join(DAGS_DIR.resolve(), "sql/radiant/sequencing_experiment_insert.sql")
            with open(_path) as f_in:
                _sql = jinja2.Template(f_in.read()).render({"mapping": get_radiant_mapping(dag_conf)})

            conn = BaseHook.get_connection("starrocks_conn")

            with conn.get_hook().get_conn().cursor() as cursor:
                cursor.executemany(
                    _sql,
                    sequencing_experiment,
                )

        insert_new_sequencing_experiment(assigned_partitions)

    updated_deleted_sequencing_experiment = RadiantStarRocksOperator(
        task_id="update_sequencing_experiment_deleted",
        sql="./sql/radiant/sequencing_experiment_update_deleted.sql",
        task_display_name="[StarRocks] Flag sequencing experiment to delete",
        trigger_rule="none_failed",
    )

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
        from airflow.operators.python import get_current_context

        context = get_current_context()
        dag_conf = context["dag_run"].conf or {}
        return [{**dag_conf, **{"part": part}} for part in prioritized]

    @task.short_circuit(
        task_id="prepare_tenants_tables",
        task_display_name="[PyOp/StarRocks] Prepare Tenants Tables",
        retries=2,
        retry_delay=datetime.timedelta(seconds=30),
    )
    def prepare_tenants_tables(sequencing_experiment_to_process: Any) -> Any:
        import os

        import jinja2
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context

        from radiant.dags import DAGS_DIR
        from radiant.tasks.data.radiant_tables import STARROCKS_RADIANT_PER_TENANT_MAPPING, get_radiant_mapping

        tenants = {s["tenant_code"] for s in sequencing_experiment_to_process}
        if not tenants:
            logger.info("No tenants to prepare, stopping the execution.")
            return []

        context = get_current_context()
        dag_conf = context["dag_run"].conf or {}
        _init_dir = os.path.join(DAGS_DIR.resolve(), "sql", "radiant", "init")

        per_tenant_tables = [key.removeprefix("starrocks_") for key in STARROCKS_RADIANT_PER_TENANT_MAPPING]
        sql_templates = {}
        for table in per_tenant_tables:
            with open(os.path.join(_init_dir, f"{table}_create_table.sql")) as f_in:
                sql_templates[table] = f_in.read()

        # Database creation is handled by the portal API, we assume the DBs exist.
        # Any error causes task failure, we want to fail-fast here to avoid starting the ETL with a broken
        # set of tables.
        conn = BaseHook.get_connection("starrocks_conn")
        with conn.get_hook().get_conn().cursor() as cursor:
            for tenant in sorted(tenants):
                mapping = get_radiant_mapping(dag_conf, tenant_code=tenant)
                for key, sql in sql_templates.items():
                    _sql = jinja2.Template(sql).render({"mapping": mapping})
                    cursor.execute(_sql)
                    logger.info(f"Prepared [{tenant}_tenant.{key}] table.")

        return sorted(tenants)

    priority = assign_priority(fetch_sequencing_experiment.output)
    prepare_tenants = prepare_tenants_tables(fetch_sequencing_experiment.output)

    import_parts = TriggerDagRunOperator.partial(
        task_id="import_part",
        task_display_name="[DAG] Import Parts in priority",
        trigger_dag_id=f"{NAMESPACE}-import-part",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        pool="import_part",
        map_index_template="Partition: {{ task.conf['part'] }}",
    ).expand(conf=priority)

    run_data_integrity_checks = TriggerDagRunOperator(
        task_id="data_integrity_checks",
        task_display_name="[DAG] Data Integrity Checks (StarRocks)",
        trigger_dag_id=f"{NAMESPACE}-data-integrity-starrocks",
        reset_dag_run=True,
        wait_for_completion=True,
    )

    (
        start
        >> tg_partition_group
        >> updated_deleted_sequencing_experiment
        >> fetch_sequencing_experiment
        >> prepare_tenants
        >> import_parts
        >> run_data_integrity_checks
    )


import_radiant()
