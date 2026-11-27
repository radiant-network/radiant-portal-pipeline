import datetime
import logging
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState
from airflow.utils.task_group import TaskGroup

from radiant.dags import DEFAULT_ARGS, NAMESPACE
from radiant.tasks.locking import STALE_LOCK_MAX_AGE
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions, rows_output_processor

logger = logging.getLogger(__name__)

std_submit_task_opts = SubmitTaskOptions(max_query_timeout=3600, poll_interval=10)

# Retried: the run's state ignores this teardown, so a failed write is only seen on its own task.
JOURNAL_RETRIES = 2
JOURNAL_RETRY_DELAY = datetime.timedelta(minutes=1)


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

    fetch_import_facts = RadiantStarRocksOperator(
        task_id="fetch_import_facts",
        sql="./sql/clinical/import_facts_select.sql",
        task_display_name="[StarRocks] Facts of the import candidates",
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="decide_import", task_display_name="[PyOp] Decide the import")
    def decide_import(fact_rows: Any) -> dict:
        from collections import Counter

        from radiant.tasks.importing.resolve import decide

        decision = decide(fact_rows)
        logger.info("%d item(s) %s", len(decision.entries), dict(Counter(str(e.decision) for e in decision.entries)))
        return decision.model_dump(mode="json")

    @task(task_id="journal_import", task_display_name="[PyOp] Journal the import decisions")
    def journal_import(decided: dict) -> int:
        """Writes the journal only: nothing waits for it."""
        from airflow.sdk import get_current_context

        from radiant.tasks.journal import Entry, record

        context = get_current_context()
        written = record([Entry(**e) for e in decided["entries"]], context["run_id"], context["dag_run"].conf)
        logger.info("%d row(s) written", len(written))
        return len(written)

    decided = decide_import(fetch_import_facts.output)
    journal_import(decided)

    with TaskGroup(group_id="partitioner_group") as tg_partition_group:

        @task(task_id="assign_partitions", task_display_name="[PyOp] Assign Partitions")
        def assign_partitions(decided: dict) -> Any:
            from radiant.tasks.starrocks.partition import SequencingExperimentPartitionAssigner

            assigner = SequencingExperimentPartitionAssigner()
            return [row.__dict__ for row in assigner.assign_partitions(decided["new_rows"])]

        assigned_partitions = assign_partitions(decided)

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
            from airflow.sdk import get_current_context

            from radiant.dags import DAGS_DIR
            from radiant.tasks.data.radiant_tables import get_radiant_mapping

            if not sequencing_experiment:
                logger.info("No new sequencing experiment to insert.")
                return

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

    flag_deleted_sequencing_experiment = RadiantStarRocksOperator(
        task_id="flag_deleted_sequencing_experiment",
        sql="./sql/radiant/sequencing_experiment_update_deleted.sql",
        task_display_name="[StarRocks] Flag the sequencing experiments of deleted tasks",
        parameters={
            "task_ids": "{{ (ti.xcom_pull(task_ids='decide_import') or {}).get('deleted_task_ids') or [-1] }}"
        },
    )

    @task.short_circuit(task_id="assign_priority", task_display_name="[PyOp] Compute Priority")
    def assign_priority(assigned: Any, decided: dict) -> Any:
        from airflow.sdk import get_current_context

        from radiant.tasks.starrocks.partition import SequencingExperimentPriorityAssigner

        prioritized = SequencingExperimentPriorityAssigner.assign_priorities([*assigned, *decided["staged_rows"]])
        dag_conf = get_current_context()["dag_run"].conf or {}
        return [{**dag_conf, **{"part": part}} for part in prioritized]

    @task.short_circuit(
        task_id="prepare_tenants_tables",
        task_display_name="[PyOp/StarRocks] Prepare Tenants Tables",
        retries=2,
        retry_delay=datetime.timedelta(seconds=30),
    )
    def prepare_tenants_tables(assigned: Any, decided: dict) -> Any:
        import os

        import jinja2
        from airflow.hooks.base import BaseHook
        from airflow.sdk import get_current_context

        from radiant.dags import DAGS_DIR
        from radiant.tasks.data.radiant_tables import STARROCKS_RADIANT_PER_TENANT_MAPPING, get_radiant_mapping

        tenants = {s["tenant_code"] for s in [*assigned, *decided["staged_rows"]]}
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

    priority = assign_priority(assigned_partitions, decided)
    prepare_tenants = prepare_tenants_tables(assigned_partitions, decided)

    import_parts = TriggerDagRunOperator.partial(
        task_id="import_part",
        task_display_name="[DAG] Import Parts in priority",
        trigger_dag_id=f"{NAMESPACE}-import-part",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        pool="import_part",
        execution_timeout=STALE_LOCK_MAX_AGE,
        map_index_template="Partition: {{ task.conf['part'] }}",
    ).expand(conf=priority)

    run_data_integrity_checks = TriggerDagRunOperator(
        task_id="data_integrity_checks",
        task_display_name="[DAG] Data Integrity Checks (StarRocks)",
        trigger_dag_id=f"{NAMESPACE}-data-integrity-starrocks",
        reset_dag_run=True,
        wait_for_completion=True,
        # The data is already loaded when the checks run: a failing check fails the integrity DAG
        # run (alerts, TestQuality), not the import.
        allowed_states=[DagRunState.SUCCESS, DagRunState.FAILED],
        failed_states=[],
        # Only test the tenants this run actually touched. Renders to a real list because the
        # DAG sets render_template_as_native_obj. prepare_tenants_tables is a short_circuit, so
        # a run with no tenants never reaches this task.
        conf={"tenants": "{{ ti.xcom_pull(task_ids='prepare_tenants_tables') }}"},
    )

    @task(
        task_id="journal_import_outcomes",
        task_display_name="[PyOp] Journal what the import did with its items",
        retries=JOURNAL_RETRIES,
        retry_delay=JOURNAL_RETRY_DELAY,
    )
    def journal_import_outcomes() -> int:
        """`failed` for each item decided `run` that the facts still hold, `succeeded` for the others."""
        from airflow.sdk import get_current_context

        from radiant.tasks.importing.resolve import fetch_facts, outcome_entries
        from radiant.tasks.journal import record

        context = get_current_context()
        conf = context["dag_run"].conf or {}
        decided = context["ti"].xcom_pull(task_ids="decide_import") or {}
        entries = outcome_entries(decided.get("entries", []), fetch_facts(conf))
        written = record(entries, context["run_id"], conf)
        logger.info("journaled %d outcome(s)", len(written))
        return len(written)

    (
        start
        >> fetch_import_facts
        >> decided
        >> tg_partition_group
        >> flag_deleted_sequencing_experiment
        >> prepare_tenants
        >> import_parts
        >> run_data_integrity_checks
    )
    flag_deleted_sequencing_experiment >> priority
    import_parts >> journal_import_outcomes().as_teardown()


import_radiant()
