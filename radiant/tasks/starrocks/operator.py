import os
import time
import uuid
from abc import abstractmethod
from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import BaseSQLOperator, SQLExecuteQueryOperator

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping
from radiant.tasks.starrocks.trigger import (
    StarRocksTaskCompleteTrigger,
)
from radiant.tasks.vcf.experiment import Case

STARROCKS_INSERT_POOL = "starrocks_insert_pool"
STARROCKS_TASK_TEMPLATE = "Radiant_Operator_Task_{uid}"


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


class RadiantStarRocksBaseOperator(BaseSQLOperator):
    def __init__(
        self,
        *,
        submit_task_options: SubmitTaskOptions = None,
        **kwargs,
    ):
        conn_id = "starrocks_conn"
        self.submit_task_options = submit_task_options

        super().__init__(
            conn_id=conn_id,
            **kwargs,
        )

    def prepare_template_context(self, context):
        dag_conf_params = context.get("dag_run").conf or {}
        dynamic_mapping = get_radiant_mapping(dag_conf_params)

        return {**context, "mapping": dynamic_mapping}

    def render_template_fields(self, context, jinja_env=None):
        _context = self.prepare_template_context(context)
        super().render_template_fields(_context, jinja_env)

    @staticmethod
    def _prepare_sql(
        sql: str,
        submit_task_options: SubmitTaskOptions = None,
    ) -> tuple[str, str]:
        """
        Prepare the SQL query with the given parameters.

        Args:
            sql (str): The SQL query to be executed.
            submit_task_options (SubmitTaskOptions): The submit task options.


        Returns:
            tuple[str, str]: The formatted SQL query and the task name.
        """
        _task_name = None
        _sql = None

        if submit_task_options:
            _task_name = STARROCKS_TASK_TEMPLATE.format(uid=str(uuid.uuid4())[-8:])
            _submit_config = f"""
            query_timeout={submit_task_options.max_query_timeout}, 
            enable_spill={submit_task_options.enable_spill}, 
            spill_mode={submit_task_options.spill_mode}
            """

            if submit_task_options.extra_args:
                _extra_args = ", ".join([f"{key}={value}" for key, value in submit_task_options.extra_args.items()])
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

    def submit_query(self, sql, method_name, parameters: MutableMapping | None = None):
        submit_sql, task_name = self._prepare_sql(
            sql=sql,
            submit_task_options=self.submit_task_options,
        )
        self.get_db_hook().run(sql=submit_sql, autocommit=True, parameters=parameters)
        if self.submit_task_options:
            # Defer until StarRocks task completes
            self.defer(
                trigger=StarRocksTaskCompleteTrigger(
                    conn_id=self.conn_id,
                    task_name=task_name,
                    sleep_time=30,
                ),
                method_name=method_name,
            )


class RadiantStarRocksOperator(RadiantStarRocksBaseOperator, SQLExecuteQueryOperator):
    """
    Custom Airflow operator to execute SQL queries on StarRocks with task submission and polling capabilities.

    Args:
        submit_task (bool): Flag to indicate if the task should be submitted.
        submit_task_options (SubmitTaskOptions): Options for submitting the task.
    """

    def __init__(
        self,
        submit_task_options: SubmitTaskOptions = None,
        **kwargs,
    ):
        super().__init__(
            **kwargs,
        )

        self.submit_task_options = submit_task_options

    def execute(self, context):
        """
        Execute the SQL query. If submit_task is True, submit the task and defer until completion.

        Args:
            context (dict): The execution context.
        """
        if self.submit_task_options:
            self.submit_query(sql=self.sql, method_name="_is_complete", parameters=self.parameters)
            return None
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


class SwapPartition:
    def __init__(
        self,
        partition: str,
        copy_partition_sql: str,
        temp_partition: str = None,
        *args,
        **kwargs,
    ):
        self.partition = partition
        self.temp_partition = temp_partition or f"tp{partition}"
        self.copy_partition_sql = copy_partition_sql


class RadiantStarRocksBasePartitionSwapOperator(RadiantStarRocksBaseOperator):
    template_fields = RadiantStarRocksBaseOperator.template_fields + (
        "table",
        "partition",
        "temp_partition",
        "copy_partition_sql",
        "parameters",
    )
    template_ext = (".sql", ".json")
    template_fields_renderers = {
        **RadiantStarRocksBaseOperator.template_fields_renderers,
        "copy_partition_sql": "sql",
        "insert_partition_sql": "sql",
        "parameters": "json",
    }

    def __init__(
        self,
        *,
        table: str,
        swap_partition: SwapPartition = None,
        submit_task_options: SubmitTaskOptions = None,
        parameters: MutableMapping | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.is_swap = swap_partition is not None
        if swap_partition:
            self.partition = swap_partition.partition
            self.temp_partition = swap_partition.temp_partition
            self.copy_partition_sql = swap_partition.copy_partition_sql
        else:
            self.partition = None
            self.temp_partition = None
            self.copy_partition_sql = None
        self.submit_task_options = submit_task_options
        self.parameters = parameters

    def prepare_query_context(self, context):
        local_vars = {
            "table": self.table,
            "partition": self.partition,
            "temp_partition": self.temp_partition,
        }
        return {**context, **local_vars}

    def render_template_fields(self, context, jinja_env=None):
        _context = super().prepare_template_context(context)
        self.parameters = self.render_template(self.parameters, _context, jinja_env)
        self.table = self.render_template(self.table, _context, jinja_env)
        self.partition = self.render_template(self.partition, _context, jinja_env)
        self.temp_partition = self.render_template(self.temp_partition, _context, jinja_env)
        _sql_context = self.prepare_query_context(_context)
        self.copy_partition_sql = self.render_template(self.copy_partition_sql, _sql_context, jinja_env)

    def prepare_partition(self, context):
        hook = self.get_db_hook()
        self.log.info(
            "Prepare partition for table: %s, partition: %s, temp_partition: %s",
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
            if self.submit_task_options:
                self.submit_query(
                    sql=_copy_partition_sql, method_name="after_copy_partition_complete", parameters=self.parameters
                )
            else:
                hook.run(sql=_copy_partition_sql, autocommit=True, parameters=self.parameters)
                self.run_insert(context, partition_exists=True)
        else:
            # If no existing partition, just run the rest
            self.run_insert(context, partition_exists=False)

    def after_copy_partition_complete(self, context, event: dict[str, Any] | None = None):
        if not event.get("status") == "success":
            raise AirflowException(
                "Copy to temp partition failed with error: %s", event.get("error_message", "Unknown error")
            )
        self.run_insert(context, partition_exists=True)

    def execute(self, context):
        if self.is_swap:
            return self.prepare_partition(context)
        else:
            return self.run_insert(context, partition_exists=False)

    @abstractmethod
    def run_insert(self, context, partition_exists):
        pass

    def swap_partitions(self):
        hook = self.get_db_hook()
        _full_partition_name = f"p{self.partition}"
        self.log.info(
            "Swapping partitions for table: %s, partition: %s, temp_partition: %s",
            self.table,
            _full_partition_name,
            self.temp_partition,
        )
        swap_partition_sql = f"""
                ALTER TABLE {self.table}
                REPLACE PARTITION ({_full_partition_name}) WITH TEMPORARY PARTITION ({self.temp_partition});
            """
        hook.run(sql=swap_partition_sql, autocommit=True)
        self.log.info("Partition swap complete")


class RadiantStarRocksPartitionSwapOperator(RadiantStarRocksBasePartitionSwapOperator):
    template_fields = RadiantStarRocksBasePartitionSwapOperator.template_fields + ("insert_partition_sql",)

    template_fields_renderers = {
        **RadiantStarRocksBasePartitionSwapOperator.template_fields_renderers,
        "insert_partition_sql": "sql",
    }

    def __init__(
        self,
        insert_partition_sql: str = "",
        *args,
        **kwargs,
    ):
        self.insert_partition_sql = insert_partition_sql
        super().__init__(*args, **kwargs)

    def render_template_fields(self, context, jinja_env=None):
        # First normal render
        super().render_template_fields(context, jinja_env)
        _context = super().prepare_template_context(context)
        _sql_context = self.prepare_query_context(_context)
        self.insert_partition_sql = self.render_template(self.insert_partition_sql, _sql_context, jinja_env)

    def run_insert(self, context, partition_exists):
        hook = self.get_db_hook()
        insert_statement = (
            f"""INSERT INTO {self.table} TEMPORARY PARTITION ({self.temp_partition})"""
            if partition_exists
            else f"""INSERT INTO {self.table}"""
        )
        _sql = f"""
            {insert_statement}
            {self.insert_partition_sql}
        """
        if self.submit_task_options:
            method_name = (
                "after_insert_sql_partition_exists_complete" if partition_exists else "after_insert_sql_complete"
            )
            self.submit_query(sql=_sql, method_name=method_name, parameters=self.parameters)
        else:
            self.log.info("Executing insert SQL on temp partition")
            hook.run(sql=_sql, autocommit=True, parameters=self.parameters)
            if partition_exists:
                self.swap_partitions()

    def after_insert_sql_complete(self, context, event: dict[str, Any] | None = None):
        if not event.get("status") == "success":
            raise AirflowException(
                "Copy to temp partition failed with error: %s", event.get("error_message", "Unknown error")
            )
        self.log.info("User SQL execution complete")

    def after_insert_sql_partition_exists_complete(self, context, event: dict[str, Any] | None = None):
        self.after_insert_sql_complete(context, event)
        self.swap_partitions()


class RadiantStarrocksLoadBaseOperator(RadiantStarRocksBasePartitionSwapOperator):
    template_fields = RadiantStarRocksBasePartitionSwapOperator.template_fields

    template_fields_renderers = {**RadiantStarRocksBasePartitionSwapOperator.template_fields_renderers}

    def __init__(
        self,
        *,
        broker_load_timeout: int = 7200,
        **kwargs,
    ):
        super().__init__(**kwargs)  # <-- forward cleanly

        self.broker_load_timeout = broker_load_timeout

        if os.getenv("STARROCKS_BROKER_USE_INSTANCE_PROFILE", "false").lower() == "true":
            self.broker_configuration = f"""
                'aws.s3.use_instance_profile' = 'true',
                'aws.s3.region' = '{os.getenv("AWS_REGION", "us-east-1")}'
            """
        else:
            self.broker_configuration = f"""
                'aws.s3.region' = '{os.getenv("AWS_REGION", "us-east-1")}',
                'aws.s3.endpoint' = '{os.getenv("AWS_ENDPOINT_URL", "s3.amazonaws.com")}',
                'aws.s3.enable_path_style_access' = 'true',
                'aws.s3.access_key' = '{os.getenv("AWS_ACCESS_KEY_ID", "access_key")}',
                'aws.s3.secret_key' = '{os.getenv("AWS_SECRET_ACCESS_KEY", "secret_key")}'
            """

    def prepare_load_context(self, context):
        _context = super().prepare_query_context(context)
        _database_name = self.table.split(".")[0]
        _table_name = self.table.split(".")[1]

        local_vars = {
            "broker_configuration": self.broker_configuration,
            "broker_load_timeout": self.broker_load_timeout,
            "database_name": _database_name,
            "table_name": _table_name,
            "temporary_partition_clause": "{{temporary_partition_clause}}",
            # to be filled during execution if partition exists
        }
        return {**_context, **local_vars}

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env)
        _context = self.prepare_template_context(context)
        self.table = self.render_template(self.table, _context, jinja_env)

    def verify_load_status(self, label):
        hook = self.get_db_hook()
        with hook.get_conn().cursor() as cursor:
            _i = 0
            while True:
                cursor.execute(f"SELECT STATE FROM information_schema.loads WHERE LABEL = '{label}'")
                load_state = cursor.fetchone()
                self.log.info(f"Load state for label {label}: {load_state}")
                if not load_state or load_state[0] == "FINISHED":
                    break
                if load_state[0] == "CANCELLED":
                    raise RuntimeError(f"Load for label {label} was cancelled.")
                time.sleep(2)
                _i += 1
                if _i > self.broker_load_timeout:
                    # Should not happen due to broker_load_timeout setting, but just in case
                    raise TimeoutError(f"Load for label {label} did not finish in time.")

    @abstractmethod
    def run_insert(self, context, partition_exists):
        pass


class RadiantStarrocksLoadOperator(RadiantStarrocksLoadBaseOperator):
    template_fields = RadiantStarrocksLoadBaseOperator.template_fields + ("sql", "load_label")

    template_fields_renderers = {**RadiantStarrocksLoadBaseOperator.template_fields_renderers, "sql": "sql"}

    def __init__(
        self,
        *,
        sql: str,
        load_label: str = "{{ task_instance_key_str }}_{{ task_instance.try_number }}",
        truncate: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.sql = sql
        self.truncate = truncate
        self.load_label = load_label

    def execute(self, context):
        hook = self.get_db_hook()
        if self.truncate:
            self.log.info(f"Truncating table {self.table} before load")
            hook.run(sql=f"TRUNCATE TABLE {self.table}", autocommit=True)
        return super().execute(context)

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env)
        _context = super().prepare_load_context(context)
        self.load_label = self.render_template(self.load_label, _context, jinja_env)
        _sql_context = {**_context, "load_label": self.load_label}
        self.sql = self.render_template(self.sql, _sql_context, jinja_env)

    def run_insert(self, context, partition_exists):
        label = self.run_load(context, partition_exists)
        self.verify_load_status(label)
        if partition_exists and self.is_swap:
            self.swap_partitions()

    def run_load(self, context, partition_exists) -> str:
        hook = self.get_db_hook()
        _temporary_partition_clause = (
            f"TEMPORARY PARTITION ({self.temp_partition})" if partition_exists and self.is_swap else ""
        )
        self.sql = self.render_template(self.sql, {"temporary_partition_clause": _temporary_partition_clause}, None)
        hook.run(sql=self.sql, autocommit=True, parameters=self.parameters)
        return self.load_label


class RadiantLoadExomiserOperator(RadiantStarrocksLoadBaseOperator):
    template_fields = RadiantStarrocksLoadBaseOperator.template_fields + ("sql", "cases")
    template_ext = (".sql", ".json")
    template_fields_renderers = {**RadiantStarrocksLoadBaseOperator.template_fields_renderers, "sql": "sql"}

    def __init__(self, cases: list[dict], *args, **kwargs):
        self.cases = cases
        self.sql = "sql/radiant/staging_exomiser_load.sql"
        super().__init__(*args, **kwargs)

    def run_load(self, context, partition_exists) -> str:
        pass

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env)
        self.cases = self.render_template(self.cases, context, jinja_env)
        _context = super().prepare_load_context(context)
        _sql_context = {**_context, "load_label": "{{load_label}}"}
        self.sql = self.render_template(self.sql, _sql_context, jinja_env)

    def run_insert(self, context, partition_exists):
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
                        "load_label": f"load_exomiser_"
                        f"{_case.case_id}_{exp.seq_id}_{exp.task_id}_{str(uuid.uuid4().hex)}",
                    }
                )

        if not _parameters:
            self.log.info("No Exomiser files to load, skipping...")
            return

        hook = self.get_db_hook()
        _temporary_partition_clause = f"TEMPORARY PARTITION ({self.temp_partition})" if partition_exists else ""
        with hook.get_conn().cursor() as cursor:
            for _params in _parameters:
                self.log.info(f"Loading Exomiser file {_params['tsv_filepath']}...")
                _path = os.path.join(DAGS_DIR.resolve(), "sql/radiant/staging_exomiser_load.sql")
                _sql_context = {
                    "load_label": _params["load_label"],
                    "temporary_partition_clause": _temporary_partition_clause,
                }
                _sql = self.render_template(self.sql, _sql_context, None)
                _sql_parameters = {**self.parameters, **_params}
                self.log.info(f"Executing exomiser load with params: {_params}")
                cursor.execute(_sql, _sql_parameters)
                self.verify_load_status(_params["load_label"])
        if partition_exists and self.is_swap:
            self.swap_partitions()
