import os
import time
import uuid
from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import Any

import jinja2
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping
from radiant.tasks.starrocks.trigger import (
    StarRocksTaskCompleteTrigger,
)
from radiant.tasks.vcf.experiment import Case

STARROCKS_INSERT_POOL = "starrocks_insert_pool"
STARROCKS_TASK_TEMPLATE = "StarRocksSQLExecuteQueryOperator_Task_{uid}"


@dataclass
class SubmitTaskOptions:
    """
    Data class to hold options for submitting a task.

    Attributes:
        max_query_timeout (int): Maximum query timeout in milliseconds.
        poll_interval (int): Interval in seconds to poll for task completion.
        enable_spill (bool): Flag to enable or disable spilling.
        spill_mode (str): Mode of spilling, e.g., 'auto'.
    """

    max_query_timeout: int = 10000
    poll_interval: int = 30
    enable_spill: bool = True
    spill_mode: str = "auto"
    extra_args: dict[str, Any] = None


class StarRocksSQLExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    Custom Airflow operator to execute SQL queries on StarRocks with task submission and polling capabilities.

    Args:
        submit_task (bool): Flag to indicate if the task should be submitted.
        submit_task_options (SubmitTaskOptions): Options for submitting the task.
        query_params (dict): Parameters to format the SQL query.
    """

    def __init__(
        self,
        submit_task_options: SubmitTaskOptions = None,
        **kwargs,
    ):
        conn_id = "starrocks_conn"
        super().__init__(
            conn_id=conn_id,
            **kwargs,
        )
        self.submit_task = submit_task_options is not None
        self.submit_task_options = submit_task_options or SubmitTaskOptions()

    @staticmethod
    def _prepare_sql(
        sql: str,
        is_submit_task: bool,
        query_timeout: int,
        enable_spill: bool = False,
        spill_mode: str = "auto",
        extra_args: dict = None,
    ) -> tuple[str, str]:
        """
        Prepare the SQL query with the given parameters.

        Args:
            sql (str): The SQL query to be executed.
            query_timeout (int): Maximum query timeout in milliseconds.
            enable_spill (bool): Flag to enable or disable spilling.
            spill_mode (str): Mode of spilling, e.g., 'auto'.

        Returns:
            tuple[str, str]: The formatted SQL query and the task name.
        """
        _task_name = None
        _sql = None

        if is_submit_task:
            _task_name = STARROCKS_TASK_TEMPLATE.format(uid=str(uuid.uuid4())[-8:])
            _submit_config = f"""
            query_timeout={query_timeout}, 
            enable_spill={enable_spill}, 
            spill_mode={spill_mode}
            """

            if extra_args:
                _extra_args = ", ".join([f"{key}={value}" for key, value in extra_args.items()])
                _submit_config += f", {_extra_args}"

            _sql = f"""
                submit /*+set_var(
                {_submit_config}
                )*/ task {_task_name} as
                {sql}
             """
        else:
            _sql = sql

        return _sql, _task_name

    def execute(self, context):
        """
        Execute the SQL query. If submit_task is True, submit the task and defer until completion.

        Args:
            context (dict): The execution context.
        """
        self.sql, _task_name = self._prepare_sql(
            sql=self.sql,
            is_submit_task=self.submit_task,
            query_timeout=self.submit_task_options.max_query_timeout,
            enable_spill=self.submit_task_options.enable_spill,
            spill_mode=self.submit_task_options.spill_mode,
        )

        if self.submit_task:
            super().execute(context)
            self.defer(
                trigger=StarRocksTaskCompleteTrigger(
                    conn_id=self.conn_id,
                    task_name=_task_name,
                    sleep_time=self.submit_task_options.poll_interval,
                ),
                method_name="_is_complete",
            )
        else:
            return super().execute(context)

    def _is_complete(self, context, event=None) -> None:
        """
        Check if the task is complete.

        Args:
            context (dict): The execution context.
            event: The event indicating task success.
        """
        if not event.get("status") == "success":
            raise AirflowException("Submit task failed with error: %s", event.get("error_message", "Unknown error"))
        return


class RadiantStarRocksOperator(StarRocksSQLExecuteQueryOperator):
    def __init__(self, params: MutableMapping | None = None, radiant_params: dict | None = None, *args, **kwargs):
        self.radiant_params = radiant_params or {}
        super().__init__(*args, params={**(params or {}), **self.radiant_params}, **kwargs)

    def prepare_template_context(self, context):
        dag_conf_params = context.get("dag_run").conf or {}
        dynamic_mapping = get_radiant_mapping(dag_conf_params)

        params = {**self.params, **self.radiant_params, **dynamic_mapping}

        return {**context, "params": params}

    def render_template_fields(self, context, jinja_env=None):
        _context = self.prepare_template_context(context)
        return super().render_template_fields(_context, jinja_env=jinja_env)


class StarRocksPartitionSwapOperator(RadiantStarRocksOperator):
    template_fields = RadiantStarRocksOperator.template_fields + (
        "table",
        "partition",
        "temp_partition",
        "copy_partition_sql",
        "parameters",
    )

    def __init__(
        self,
        table: str,
        partition: str,
        temp_partition: str = None,
        copy_partition_sql: str = "",
        *args,
        **kwargs,
    ):
        self.table = table
        self.partition = partition
        self.temp_partition = temp_partition or f"tp{partition}"
        self.copy_partition_sql = copy_partition_sql
        super().__init__(*args, **kwargs)

    def prepare_template_context(self, context):
        _context = super().prepare_template_context(context)
        local_vars = {
            "table": self.table,
            "partition": self.partition,
            "temp_partition": self.temp_partition,
        }
        return {**_context, **local_vars}

    def render_template_fields(self, context, jinja_env=None):
        _context = self.prepare_template_context(context)

        # First normal render
        super().render_template_fields(_context, jinja_env)

        # Second render pass for nested templates
        for field_name in ("sql", "copy_partition_sql"):
            value = getattr(self, field_name)
            if isinstance(value, str):
                rendered = self.render_template(value, _context, jinja_env)
                setattr(self, field_name, rendered)

    def execute(self, context):
        hook = self.get_db_hook()
        self.log.info(
            "Executing StarRocksPartitionSwapOperator for table: %s, partition: %s, temp_partition: %s",
            self.table,
            self.partition,
            self.temp_partition,
        )

        # Check if partition exists
        partition_exists_sql = f"""
            SHOW PARTITIONS FROM {self.table} WHERE PartitionName = 'p{self.partition}';
        """
        partition_exists = hook.get_first(partition_exists_sql)

        if partition_exists:
            clean_temp_part_part_sql = f"""
            ALTER TABLE {self.table}
            DROP TEMPORARY PARTITION IF EXISTS {self.temp_partition};
            """
            hook.run(sql=clean_temp_part_part_sql, autocommit=True)
            create_temp_partition_sql = f"""
                ALTER TABLE {self.table}
                ADD TEMPORARY PARTITION IF NOT EXISTS {self.temp_partition} VALUES IN ('{self.partition}');
            """
            hook.run(sql=create_temp_partition_sql, autocommit=True)
            _copy_partition_sql = f"""
                INSERT INTO {self.table} TEMPORARY PARTITION ({self.temp_partition})
                {self.copy_partition_sql}
            """
            # Step 2: Copy data to temp partition (submit async task)
            if self.submit_task:
                self.submit_query(hook, _copy_partition_sql, "after_copy_partition_complete")
            else:
                hook.run(sql=_copy_partition_sql, autocommit=True, parameters=self.parameters)
                self.run_user_sql(context, partition_exists=True)
        else:
            # If no existing partition, just run the rest
            self.run_user_sql(context, partition_exists=False)

    def after_copy_partition_complete(self, context, event: dict[str, Any] | None = None):
        if not event.get("status") == "success":
            raise AirflowException(
                "Copy to temp partition failed with error: %s", event.get("error_message", "Unknown error")
            )
        self.run_user_sql(context, partition_exists=True)

    def run_user_sql(self, context, partition_exists):
        hook = self.get_db_hook()
        insert_statement = (
            f"""INSERT INTO {self.table} TEMPORARY PARTITION ({self.temp_partition})"""
            if partition_exists
            else f"""INSERT INTO {self.table}"""
        )
        _sql = f"""
            {insert_statement}
            {self.sql}
        """
        if self.submit_task:
            method_name = "after_user_sql_partition_exists_complete" if partition_exists else "after_user_sql_complete"
            self.submit_query(hook, _sql, method_name)
        else:
            self.log.info("Executing user SQL on temp partition")
            hook.run(sql=_sql, autocommit=True, parameters=self.parameters)
            if partition_exists:
                self.swap_partitions()

    def after_user_sql_complete(self, context, event: dict[str, Any] | None = None):
        if not event.get("status") == "success":
            raise AirflowException(
                "Copy to temp partition failed with error: %s", event.get("error_message", "Unknown error")
            )
        self.log.info("User SQL execution complete")

    def after_user_sql_partition_exists_complete(self, context, event: dict[str, Any] | None = None):
        self.after_user_sql_complete(context, event)
        self.swap_partitions()

    def swap_partitions(self):
        hook = self.get_db_hook()
        swap_partition_sql = f"""
                ALTER TABLE {self.table}
                REPLACE PARTITION (p{self.partition}) WITH TEMPORARY PARTITION ({self.temp_partition});
            """
        hook.run(sql=swap_partition_sql, autocommit=True)
        self.log.info("Partition swap complete")

    def submit_query(self, hook, sql, method_name):
        submit_sql, task_name = self._prepare_sql(
            sql=sql,
            is_submit_task=self.submit_task,
            query_timeout=self.submit_task_options.max_query_timeout,
            enable_spill=self.submit_task_options.enable_spill,
            spill_mode=self.submit_task_options.spill_mode,
        )
        hook.run(sql=submit_sql, autocommit=True, parameters=self.parameters)
        # Defer until StarRocks task completes
        self.defer(
            trigger=StarRocksTaskCompleteTrigger(
                conn_id=self.conn_id,
                task_name=task_name,
                sleep_time=self.submit_task_options.poll_interval,
            ),
            method_name=method_name,
        )


class LoadExomiserOperator(StarRocksPartitionSwapOperator):
    template_fields = StarRocksPartitionSwapOperator.template_fields + ("cases",)

    def __init__(self, cases: list[dict], *args, **kwargs):
        self.cases = cases
        super().__init__(*args, **kwargs)

    def run_user_sql(self, context, partition_exists):
        _query_params = self.params | {"broker_load_timeout": 7200}

        _path = os.path.join(DAGS_DIR.resolve(), "sql/radiant/staging_exomiser_load.sql")
        with open(_path) as f_in:
            load_staging_exomiser_sql = jinja2.Template(f_in.read()).render({"params": _query_params})

        _parameters = []
        for case in self.cases:
            _case = Case.model_validate(case)
            for exp in _case.experiments:
                if not exp.exomiser_filepath:
                    continue
                _parameters.append(
                    {
                        "part": self.partition,
                        "seq_id": exp.seq_id,
                        "tsv_filepath": exp.exomiser_filepath,
                        "label": f"load_exomiser_{_case.case_id}_{exp.seq_id}_{exp.task_id}_{str(uuid.uuid4().hex)}",
                    }
                )

        if not _parameters:
            self.log.info("No Exomiser files to load, skipping...")
            return

        if os.getenv("STARROCKS_BROKER_USE_INSTANCE_PROFILE", "false").lower() == "true":
            broker_configuration = f"""
                'aws.s3.use_instance_profile' = 'true',
                'aws.s3.region' = '{os.getenv("AWS_REGION", "us-east-1")}'
            """
        else:
            broker_configuration = f"""
                'aws.s3.region' = '{os.getenv("AWS_REGION", "us-east-1")}',
                'aws.s3.endpoint' = '{os.getenv("AWS_ENDPOINT_URL", "s3.amazonaws.com")}',
                'aws.s3.enable_path_style_access' = 'true',
                'aws.s3.access_key' = '{os.getenv("AWS_ACCESS_KEY_ID", "access_key")}',
                'aws.s3.secret_key' = '{os.getenv("AWS_SECRET_ACCESS_KEY", "secret_key")}'
            """
        hook = self.get_db_hook()

        with hook.get_conn().cursor() as cursor:
            for _params in _parameters:
                self.log.info(f"Loading Exomiser file {_params['tsv_filepath']}...")
                self.log.info(f"Executing exomiser load with params: {_params}")
                _database_name = self.table.split(".")[0]
                _table_name = self.table.split(".")[1]
                _sql = load_staging_exomiser_sql.format(
                    label=f"{_params['label']}",
                    temporary_partition_clause=f"TEMPORARY PARTITION ({self.temp_partition})"
                    if partition_exists
                    else "",
                    broker_configuration=broker_configuration,
                    database_name=_database_name,
                    table_name=_table_name,
                )
                cursor.execute(_sql, _params)

                _i = 0
                while True:
                    cursor.execute("SELECT STATE FROM information_schema.loads WHERE LABEL = %(label)s", _params)
                    load_state = cursor.fetchone()
                    self.log.info(f"Load state for label {_params['label']}: {load_state}")
                    if not load_state or load_state[0] == "FINISHED":
                        break
                    if load_state[0] == "CANCELLED":
                        raise RuntimeError(f"Load for label {_params['label']} was cancelled.")
                    time.sleep(2)
                    _i += 1
                    if _i > 30:
                        raise TimeoutError(f"Load for label {_params['label']} did not finish in time.")

        if partition_exists:
            self.swap_partitions()
