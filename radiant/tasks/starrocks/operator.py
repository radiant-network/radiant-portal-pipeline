import itertools
import os
import time
import uuid
from abc import abstractmethod
from collections.abc import MutableMapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from urllib.parse import urlparse

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.common.sql.operators.sql import BaseSQLOperator, SQLExecuteQueryOperator

from radiant.tasks.data.radiant_tables import get_radiant_mapping
from radiant.tasks.starrocks.trigger import (
    StarRocksTaskCompleteTrigger,
)
from radiant.tasks.vcf.experiment import (
    RADIANT_GERMLINE_ANNOTATION_TASK,
    RADIANT_GERMLINE_CNV_ANNOTATION_TASK,
    RadiantGermlineAnnotationTask,
    RadiantGermlineCnvAnnotationTask,
)

STARROCKS_INSERT_POOL = "starrocks_insert_pool"
STARROCKS_TASK_TEMPLATE = "Radiant_Operator_Task_{uid}"


@dataclass
class SubmitTaskOptions:
    """
    Data class to hold options for submitting a task.

    Attributes:
        max_query_timeout (int): StarRocks statement deadline, in seconds. Set as both `query_timeout` and
            `insert_timeout` on the submitted task: since StarRocks 3.4.0 `query_timeout` no longer applies to
            INSERT (nor UPDATE/DELETE/CTAS), `insert_timeout` does, and its cluster default is 14400 s
            (https://docs.starrocks.io/docs/3.4/sql-reference/System_variable/). Without `insert_timeout` an
            INSERT task outlives `defer_timeout`, Airflow fails the task on its backstop while the load keeps
            running to completion on the cluster.
        poll_interval (int): Interval in seconds to poll for task completion.
        enable_spill (bool): Flag to enable or disable spilling.
        spill_mode (str): Mode of spilling, e.g., 'auto'.
        defer_timeout (int): Deadline in seconds for the whole deferral, enforced by the scheduler rather
            than the triggerer, so a wedged triggerer fails the task instead of leaving it deferred forever.
            Defaults to `max_query_timeout` plus `DEFER_TIMEOUT_MARGIN` — StarRocks aborts the statement at
            `max_query_timeout`, so the task cannot legitimately outlive that.
        sync_wait (int): Seconds the worker polls the task before deferring, `$RADIANT_STARROCKS_SYNC_WAIT_SECONDS`
            (300 by default). A resume after a defer starts a new pod under the KubernetesExecutor, which costs
            more than most queries take.
    """

    DEFER_TIMEOUT_MARGIN = 600
    SYNC_POLL_INTERVAL = 5

    max_query_timeout: int = 14400
    poll_interval: int = 30
    enable_spill: bool = True
    spill_mode: str = "auto"
    extra_args: dict[str, Any] = None
    defer_timeout: int | None = None
    sync_wait: int = field(default_factory=lambda: int(os.getenv("RADIANT_STARROCKS_SYNC_WAIT_SECONDS", "300")))

    def get_defer_timeout(self) -> int:
        if self.defer_timeout is not None:
            return self.defer_timeout
        return self.max_query_timeout + self.DEFER_TIMEOUT_MARGIN


class RadiantStarRocksBaseOperator(BaseSQLOperator):
    def __init__(
        self,
        *,
        submit_task_options: SubmitTaskOptions = None,
        tenant_code: str | None = None,
        retries=3,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True,
        **kwargs,
    ):
        conn_id = "starrocks_conn"
        self.submit_task_options = submit_task_options
        self.tenant_code = tenant_code

        super().__init__(
            conn_id=conn_id,
            retries=retries,
            retry_delay=retry_delay,
            retry_exponential_backoff=retry_exponential_backoff,
            **kwargs,
        )

    def prepare_template_context(self, context):
        dag_conf_params = dict(context.get("dag_run").conf or {})
        tenant_code = self.tenant_code if self.tenant_code is not None else dag_conf_params.get("RADIANT_TENANT_CODE")
        dynamic_mapping = get_radiant_mapping(dag_conf_params, tenant_code=tenant_code)

        extra = {"mapping": dynamic_mapping, "tenant_code": tenant_code}
        return {**context, **extra}

    def render_template_fields(self, context, jinja_env=None):
        _context = self.prepare_template_context(context)
        super().render_template_fields(_context, jinja_env)

    def _do_render_template_fields(self, parent, template_fields, context, jinja_env=None, *args, **kwargs):
        # Mapped operators (.expand / .expand_kwargs) bypass render_template_fields and call this directly
        # (see MappedOperator.render_template_fields), so the Radiant context (mapping/tenant_code/...) must
        # be injected here too, otherwise '{{ mapping.* }}' is undefined when rendering a mapped task.
        context = self.prepare_template_context(context)
        super()._do_render_template_fields(parent, template_fields, context, jinja_env, *args, **kwargs)

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
            # Both timeouts, on purpose: `query_timeout` covers SELECT-only tasks, `insert_timeout` covers
            # INSERT/UPDATE/DELETE/CTAS, which `query_timeout` stopped governing in StarRocks 3.4.0.
            _submit_config = f"""
            query_timeout={submit_task_options.max_query_timeout},
            insert_timeout={submit_task_options.max_query_timeout},
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

    def submit_query(self, context, sql, method_name, parameters: MutableMapping | None = None):
        submit_sql, task_name = self._prepare_sql(
            sql=sql,
            submit_task_options=self.submit_task_options,
        )
        self.get_db_hook().run(sql=submit_sql, autocommit=True, parameters=parameters)
        if self.submit_task_options:
            trigger = StarRocksTaskCompleteTrigger(
                conn_id=self.conn_id,
                task_name=task_name,
                sleep_time=self.submit_task_options.poll_interval,
            )
            deadline = time.monotonic() + self.submit_task_options.sync_wait
            while time.monotonic() < deadline:
                event = trigger.poll_sync()
                if event is not None:
                    return getattr(self, method_name)(context, event.payload)
                time.sleep(SubmitTaskOptions.SYNC_POLL_INTERVAL)

            # Defer until StarRocks task completes. `timeout` is the scheduler-side backstop: without it a
            # triggerer that dies or wedges leaves the task deferred indefinitely.
            self.defer(
                trigger=trigger,
                method_name=method_name,
                timeout=timedelta(seconds=self.submit_task_options.get_defer_timeout()),
            )


def rows_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    """`output_processor` for `RadiantStarRocksOperator`: the cursor rows as a list of dicts keyed by
    column name, so the XCom is `[{column: value}, ...]`."""
    column_names = [desc[0] for desc in descriptions[0]]
    return [[dict(zip(column_names, row, strict=False)) for row in results[0]]]


class RadiantStarRocksOperator(RadiantStarRocksBaseOperator, SQLExecuteQueryOperator):
    """
    Custom Airflow operator to execute SQL queries on StarRocks with task submission and polling capabilities.

    Args:
        submit_task (bool): Flag to indicate if the task should be submitted.
        submit_task_options (SubmitTaskOptions): Options for submitting the task.
    """

    template_fields = SQLExecuteQueryOperator.template_fields + ("skip_if",)

    def __init__(
        self,
        submit_task_options: SubmitTaskOptions = None,
        skip_if: bool | str | None = None,
        **kwargs,
    ):
        super().__init__(
            **kwargs,
        )

        self.submit_task_options = submit_task_options
        self.skip_if = skip_if

    def execute(self, context):
        """
        Execute the SQL query. If submit_task is True, submit the task and defer until completion.

        Args:
            context (dict): The execution context.
        """
        skip = self.skip_if
        if isinstance(skip, str):
            skip = skip.strip().lower() not in ("", "false", "none", "0")
        if skip:
            raise AirflowSkipException(f"skip_if resolved truthy ({self.skip_if!r}) -- nothing to execute.")

        if self.submit_task_options:
            self.submit_query(context, sql=self.sql, method_name="_is_complete", parameters=self.parameters)
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
        if event.get("status") != "success":
            err = event.get("error_message", "Unknown error")
            raise AirflowException(f"Submit task failed with error: {err}")

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

    def _do_render_template_fields(self, parent, template_fields, context, jinja_env=None, *args, **kwargs):
        # Mapped path entrypoint: run the operator's staged rendering (resolves mapping, table, then the
        # partition-scoped SQL from the rendered table) instead of the generic per-field loop, which can't
        # honour the ordering. Safe: render_template_fields renders fields explicitly, never re-enters here.
        self.render_template_fields(context, jinja_env)

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
            f"Prepare partition for table: "
            f"{self.table}, "
            f"partition: {self.partition}, "
            f"temp_partition: {self.temp_partition}"
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
                    context,
                    sql=_copy_partition_sql,
                    method_name="after_copy_partition_complete",
                    parameters=self.parameters,
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
                f"Copy to temp partition failed with error: {event.get('error_message', 'Unknown error')}",
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
            f"Swapping partitions for table: "
            f"{self.table}, "
            f"partition: {_full_partition_name}, "
            f"temp_partition: {self.temp_partition}"
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
            self.submit_query(context, sql=_sql, method_name=method_name, parameters=self.parameters)
        else:
            self.log.info("Executing insert SQL on temp partition")
            hook.run(sql=_sql, autocommit=True, parameters=self.parameters)
            if partition_exists:
                self.swap_partitions()

    def after_insert_sql_complete(self, context, event: dict[str, Any] | None = None):
        if not event.get("status") == "success":
            raise AirflowException(
                f"Copy to temp partition failed with error: {event.get('error_message', 'Unknown error')}",
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


# The annotation tasks that carry an Exomiser report, each with its task model, its load and its load label prefix.
# The SNV report goes to `raw_exomiser`, the CNV report (RAD-181) to `raw_exomiser_cnv`: one operator per task type,
# so a report is only ever loaded into its own staging table.
EXOMISER_LOADS = {
    RADIANT_GERMLINE_ANNOTATION_TASK: (
        RadiantGermlineAnnotationTask,
        "sql/radiant/staging_exomiser_load.sql",
        "load_exomiser",
    ),
    RADIANT_GERMLINE_CNV_ANNOTATION_TASK: (
        RadiantGermlineCnvAnnotationTask,
        "sql/radiant/staging_exomiser_cnv_load.sql",
        "load_exomiser_cnv",
    ),
}


# The reports of one load job (one `data_desc` each). A WXS part (up to 1000 tasks) loads in about 10 jobs, not 1000.
EXOMISER_FILES_PER_LOAD = 100


class RadiantLoadExomiserOperator(RadiantStarrocksLoadBaseOperator):
    """Load the Exomiser report of each annotation task of the run into the shared staging table.

    One broker load job per `files_per_load` reports, one `data_desc` per report, each tagged in its SET with its task,
    the proband's seq_id and tenant. A job is atomic: a bad report cancels the batch and fails the task, before the
    swap, so that Airflow retries it and the rows of the partition stay.
    """

    template_fields = RadiantStarrocksLoadBaseOperator.template_fields + ("sql", "tasks")
    template_ext = (".sql", ".json")
    template_fields_renderers = {**RadiantStarrocksLoadBaseOperator.template_fields_renderers, "sql": "sql"}

    def __init__(
        self,
        tasks: list[dict],
        *args,
        annotation_task_type: str = RADIANT_GERMLINE_ANNOTATION_TASK,
        files_per_load: int = EXOMISER_FILES_PER_LOAD,
        **kwargs,
    ):
        # Not `task_type`: BaseOperator already has a `task_type` property (the operator's class name).
        if annotation_task_type not in EXOMISER_LOADS:
            raise ValueError(
                f"No Exomiser load for task type [{annotation_task_type}], expected one of {sorted(EXOMISER_LOADS)}."
            )
        if files_per_load < 1:
            raise ValueError(f"files_per_load must be at least 1, got {files_per_load}.")
        self.tasks = tasks
        self.annotation_task_type = annotation_task_type
        self.files_per_load = files_per_load
        self.task_model, self.sql, self.label_prefix = EXOMISER_LOADS[annotation_task_type]
        super().__init__(*args, **kwargs)

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env)
        self.tasks = self.render_template(self.tasks, context, jinja_env)
        _context = super().prepare_load_context(context)
        # Filled per load job in load_files; the data_desc loop is a `raw` block of the SQL, rendered there too.
        _sql_context = {**_context, "load_label": "{{load_label}}"}
        self.sql = self.render_template(self.sql, _sql_context, jinja_env)

    def load_files(self, cursor, files: list[dict], temporary_partition_clause: str) -> None:
        """Load the reports of one batch in one load job, one `data_desc` per report, and wait for it."""
        first = files[0]
        load_label = f"{self.label_prefix}_{first['task_id']}_{first['seq_id']}_{len(files)}_{uuid.uuid4().hex}"
        filepaths = [f["filepath"] for f in files]
        self.log.info(f"Loading {len(files)} Exomiser file(s) in load {load_label}: {filepaths}")
        _sql = self.render_template(
            self.sql,
            {
                "file_count": len(files),
                "load_label": load_label,
                "table_name": self.table.split(".")[1],
                "temporary_partition_clause": temporary_partition_clause,
            },
            None,
        )
        _parameters = {"part": self.partition}
        for i, file in enumerate(files):
            _parameters.update({f"{key}_{i}": file[key] for key in ("filepath", "seq_id", "task_id", "tenant_code")})
        cursor.execute(_sql, _parameters)
        self.verify_load_status(load_label)

    def run_insert(self, context, partition_exists):
        files = []
        for task in self.tasks:
            # A deleted task's rows are dropped by the copy step and never read by the insert, and its file may be
            # gone from S3: loading it could only fail the part.
            if (
                task.get("task_type") != self.annotation_task_type
                or task.get("deleted")
                or not task.get("exomiser_filepath")
            ):
                continue

            _task = self.task_model.model_validate(task)
            # The annotation task holds every family member; the Exomiser report ranks the proband.
            _proband = next((e for e in _task.experiments if e.family_role == "proband"), None)
            if _proband is None:
                raise ValueError(f"No proband found in task [{_task.task_id}] for the Exomiser load.")
            files.append(
                {
                    "seq_id": _proband.seq_id,
                    "task_id": _task.task_id,
                    # Tag rows with the tenant in the shared staging table; insert_exomiser routes by it later.
                    "tenant_code": _proband.tenant_code,
                    "filepath": _task.exomiser_filepath,
                }
            )

        if not files:
            self.log.info("No Exomiser files to load, skipping...")
            # The copy step has already filled the temporary partition with the rows to keep (the tasks not in this
            # run). Swap it in all the same, or the rows of this run's tasks (deleted, or re-imported without a
            # report) stay in the partition for good.
            if partition_exists and self.is_swap:
                self.swap_partitions()
            return

        hook = self.get_db_hook()
        _temporary_partition_clause = f"TEMPORARY PARTITION ({self.temp_partition})" if partition_exists else ""
        with hook.get_conn().cursor() as cursor:
            for batch in itertools.batched(files, self.files_per_load):
                self.load_files(cursor, list(batch), _temporary_partition_clause)
        if partition_exists and self.is_swap:
            self.swap_partitions()


# RAD-315. The two DRAGEN `coverage_by_gene` CSV formats, told apart by their header (as QLIN does, clin-variant-etl
# `normalized/CoverageByGene.scala`). The broker load is positional, so the header must be exactly one of the two.
GENE_COVERAGE_COVERAGE_COLUMNS = [
    "coverage5",
    "coverage15",
    "coverage30",
    "coverage50",
    "coverage100",
    "coverage200",
    "coverage300",
    "coverage400",
    "coverage500",
    "coverage1000",
]
GENE_COVERAGE_OLD_HEADER = ["gene", "size", "average_coverage", *GENE_COVERAGE_COVERAGE_COLUMNS]
GENE_COVERAGE_NEW_HEADER = ["gene", "type", "size", "average_coverage", *GENE_COVERAGE_COVERAGE_COLUMNS]
_GENE_COVERAGE_TABLE_COLUMNS = [
    "gene",
    "size",
    "average_coverage",
    *[f"{c.replace('coverage', 'coverage_')}x" for c in GENE_COVERAGE_COVERAGE_COLUMNS],
]
# Per format: the column list of the broker load, and the value of `type` (the new format's own column, else CDS).
GENE_COVERAGE_FORMATS = {
    "old": (", ".join(_GENE_COVERAGE_TABLE_COLUMNS), "'CDS'"),
    "new": (", ".join(["gene", "type", *_GENE_COVERAGE_TABLE_COLUMNS[1:]]), "type"),
}
# The header line of a `covgene` file is its first line, read from S3 by the Airflow worker: a ranged GET of the first
# bytes (S3 API GetObject, `Range`), not a scan of the file. The broker load's `skip_header = 1` takes the same first
# line as the header. A DRAGEN header is about 170 bytes.
GENE_COVERAGE_HEADER_BYTES = 4096


def parse_gene_coverage_header(line: str) -> str:
    """Return the format (`old` or `new`) of a `coverage_by_gene` CSV from its header line.

    DRAGEN writes the first column as `#gene`. Raises ValueError for any other header: a positional load of an unknown
    column order would put the values in the wrong columns.
    """
    columns = [c.strip() for c in line.strip().split(",")]
    if columns and columns[0].startswith("#"):
        columns[0] = columns[0][1:]
    if columns == GENE_COVERAGE_OLD_HEADER:
        return "old"
    if columns == GENE_COVERAGE_NEW_HEADER:
        return "new"
    raise ValueError(
        f"Unexpected coverage_by_gene header {line.strip()[:300]!r}: expected the columns {GENE_COVERAGE_OLD_HEADER} "
        f"or {GENE_COVERAGE_NEW_HEADER}."
    )


# The files of one load job (one `data_desc` each). A WXS part (up to 1000 experiments) loads in about 10 jobs, not
# 1000, and a job a bad file cancels reloads at most this many files one by one.
GENE_COVERAGE_FILES_PER_LOAD = 100

# The S3 error codes of a file that cannot be read for good: missing, no access, or empty (a range on an empty object
# is `InvalidRange`). Any other error (throttling, network, 5xx) is not a bad file, so that the task fails.
_GENE_COVERAGE_FILE_ERROR_CODES = frozenset(
    {"NoSuchKey", "NoSuchBucket", "AccessDenied", "InvalidRange", "404", "403"}
)


def read_gene_coverage_header_line(s3_client, filepath: str) -> str:
    """The first line of a `covgene` file, from a ranged GET of its first `GENE_COVERAGE_HEADER_BYTES` bytes.

    Raises `GeneCoverageFileError` for a file S3 says cannot be read for good, and for a first line that does not end
    within the range; any other error is raised as is.
    """
    from botocore.exceptions import ClientError

    parsed = urlparse(filepath)
    if parsed.scheme not in ("s3", "s3a") or not parsed.netloc or not parsed.path.strip("/"):
        raise GeneCoverageFileError(f"Not an S3 object path: {filepath!r}.")
    try:
        response = s3_client.get_object(
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/"),
            Range=f"bytes=0-{GENE_COVERAGE_HEADER_BYTES - 1}",
        )
        data = response["Body"].read()
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code not in _GENE_COVERAGE_FILE_ERROR_CODES:
            raise
        raise GeneCoverageFileError(f"S3 cannot read the file ({code}): {e}") from e
    # utf-8-sig drops a leading byte order mark, if any: the first column stays `#gene`.
    text = data.decode("utf-8-sig", errors="replace")
    line, newline, _ = text.partition("\n")
    if not newline and len(data) >= GENE_COVERAGE_HEADER_BYTES:
        raise GeneCoverageFileError(f"No header line in the first {GENE_COVERAGE_HEADER_BYTES} bytes of the file.")
    return line


class GeneCoverageLoadError(AirflowException):
    """Raised by the gene coverage check when one or more `covgene` files could not be loaded."""


class GeneCoverageFileError(Exception):
    """One `covgene` file that cannot be loaded: missing, empty or with no access, an unknown header, or a load
    that StarRocks cancelled or did not finish in time. A cancelled load counts as a file error whatever its cause (bad
    data, missing file, BE lost, timeout): the operator reads the cause in the error that `check_gene_coverage_files`
    gives. Any other error (lost connection, SQL error) is not a file error: it fails the task before the swap, so that
    Airflow retries it and the loaded coverage stays."""


class RadiantLoadGeneCoverageOperator(RadiantStarrocksLoadBaseOperator):
    """RAD-315. Broker-load the `covgene` CSV of each current alignment into the shared `raw_gene_coverage`.

    `files` is the output of `gene_coverage_file_select.sql`: one row per alignment task (seq_id, task_id, tenant_code,
    filepath). The copy step of the partition swap keeps the rows of the experiments the run does not reload.

    The header of each file is read first, then the files of one format load together: one load job per
    `files_per_load` files (one `data_desc` per file), so that a part pays the overhead of a job per batch, not per
    file. A job is atomic: a bad file cancels it, and its files then load one by one, so that only the bad file is
    lost.

    One bad file (`GeneCoverageFileError`) does not stop the others: its error (S3 path + message) is kept, the other
    files load, and the rows already loaded for the bad file's experiment are copied back into the temporary partition,
    so that the swap keeps them. The operator returns the errors (XCom), and a later task fails the run with them,
    after the per-tenant inserts. Any other error fails the task before the swap.
    """

    template_fields = RadiantStarrocksLoadBaseOperator.template_fields + ("sql", "files")
    template_ext = (".sql", ".json")
    template_fields_renderers = {**RadiantStarrocksLoadBaseOperator.template_fields_renderers, "sql": "sql"}

    def __init__(
        self,
        files: list[dict],
        *args,
        sql: str = "sql/radiant/staging_gene_coverage_load.sql",
        files_per_load: int = GENE_COVERAGE_FILES_PER_LOAD,
        **kwargs,
    ):
        if files_per_load < 1:
            raise ValueError(f"files_per_load must be at least 1, got {files_per_load}.")
        self.files = files
        self.sql = sql
        self.files_per_load = files_per_load
        self.errors: list[dict] = []
        super().__init__(*args, **kwargs)

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env)
        self.files = self.render_template(self.files, context, jinja_env)
        _context = super().prepare_load_context(context)
        # Filled per load job in load_files; the data_desc loop is a `raw` block of the SQL, rendered there too.
        _sql_context = {**_context, "load_label": "{{load_label}}"}
        self.sql = self.render_template(self.sql, _sql_context, jinja_env)

    def execute(self, context):
        self.errors = []
        super().execute(context)
        return self.errors

    # Polls (2 s each) to wait for the load's row in information_schema.loads before the load counts as lost.
    _LOAD_ROW_POLLS = 30

    def wait_for_load(self, cursor, label: str) -> None:
        """Wait for the load `label`. A cancelled or timed-out load is a `GeneCoverageFileError` (a timed-out load is
        cancelled first, so that it cannot write after the swap). A load that never shows up is not a file error."""
        for i in range(self.broker_load_timeout):
            cursor.execute(
                "SELECT STATE, ERROR_MSG FROM information_schema.loads WHERE LABEL = %(label)s", {"label": label}
            )
            row = cursor.fetchone()
            self.log.info(f"Load state for label {label}: {row}")
            if row and row[0] == "FINISHED":
                return
            if row and row[0] == "CANCELLED":
                raise GeneCoverageFileError(f"Load {label} was cancelled: {row[1]}")
            if not row and i >= self._LOAD_ROW_POLLS:
                raise RuntimeError(f"Load {label} is not in information_schema.loads.")
            time.sleep(2)
        database_name = self.table.split(".")[0]
        cursor.execute(f"CANCEL LOAD FROM {database_name} WHERE LABEL = %(label)s", {"label": label})
        raise GeneCoverageFileError(f"Load {label} did not finish in time; it was cancelled.")

    def get_s3_client(self):
        """The S3 client of the header reads: the worker's default credential chain, as the import lock
        (`radiant/tasks/locking.py`); `AWS_ENDPOINT_URL` points it at the local S3 store."""
        import boto3

        return boto3.client("s3")

    def read_format(self, s3_client, filepath: str) -> str:
        """The format of a `covgene` file, from its first line (`read_gene_coverage_header_line`).

        A file S3 cannot read for good, or a missing or unknown header, is a file error; any other error is raised as
        is.
        """
        line = read_gene_coverage_header_line(s3_client, filepath)
        try:
            return parse_gene_coverage_header(line)
        except ValueError as e:
            raise GeneCoverageFileError(str(e)) from e

    def restore_failed_experiments(self, hook, failed_seq_ids: set, loaded_task_ids: set) -> None:
        """Copy back, into the temporary partition, the rows already loaded for the experiments of the bad files.

        The copy step dropped every experiment of the delta, so without this the swap would delete the coverage an
        earlier run loaded, and the experiment, imported all the same, would never be reloaded. The rows of a task
        loaded by this run are left out, or they would be there twice.
        """
        self.log.info(f"Keeping the loaded coverage of the experiments of the bad files: {sorted(failed_seq_ids)}")
        hook.run(
            sql=f"""
                INSERT INTO {self.table} TEMPORARY PARTITION ({self.temp_partition})
                SELECT * FROM {self.table} PARTITION (p{self.partition})
                WHERE seq_id IN %(seq_ids)s AND task_id NOT IN %(task_ids)s
            """,
            autocommit=True,
            parameters={"seq_ids": sorted(failed_seq_ids), "task_ids": sorted(loaded_task_ids) or [-1]},
        )

    def load_files(self, cursor, files: list[dict], _format: str, temporary_partition_clause: str) -> None:
        """Load the files of one format in one load job, one `data_desc` per file, and wait for it.

        The job is atomic: when it is cancelled (`GeneCoverageFileError`), none of its files wrote a row.
        """
        file_columns, type_value = GENE_COVERAGE_FORMATS[_format]
        first = files[0]
        load_label = f"load_gene_coverage_{first['task_id']}_{first['seq_id']}_{len(files)}_{uuid.uuid4().hex}"
        self.log.info(f"Loading {len(files)} {_format}-format coverage_by_gene file(s) in load {load_label}...")
        _sql = self.render_template(
            self.sql,
            {
                "load_label": load_label,
                "file_count": len(files),
                "table_name": self.table.split(".")[1],
                "temporary_partition_clause": temporary_partition_clause,
                "file_columns": file_columns,
                "type_value": type_value,
            },
            None,
        )
        _parameters = {"part": self.partition}
        for i, file in enumerate(files):
            _parameters.update(
                {
                    f"filepath_{i}": file["filepath"],
                    f"seq_id_{i}": file["seq_id"],
                    f"task_id_{i}": file["task_id"],
                    f"tenant_code_{i}": file["tenant_code"],
                }
            )
        cursor.execute(_sql, _parameters)
        self.wait_for_load(cursor, load_label)

    def run_insert(self, context, partition_exists):
        files = [f for f in (self.files or []) if f.get("filepath")]
        if not files:
            self.log.info("No coverage_by_gene file to load.")

        hook = self.get_db_hook()
        _temporary_partition_clause = f"TEMPORARY PARTITION ({self.temp_partition})" if partition_exists else ""
        failed_seq_ids, loaded_task_ids = set(), set()

        def fail(file: dict, error: GeneCoverageFileError) -> None:
            self.log.error(f"Could not load coverage_by_gene file {file['filepath']}: {error}")
            self.errors.append({"filepath": file["filepath"], "task_id": file["task_id"], "error": str(error)})
            failed_seq_ids.add(file["seq_id"])

        files_by_format: dict[str, list[dict]] = {_format: [] for _format in GENE_COVERAGE_FORMATS}
        s3_client = self.get_s3_client() if files else None
        for file in files:
            try:
                files_by_format[self.read_format(s3_client, file["filepath"])].append(file)
            except GeneCoverageFileError as e:
                fail(file, e)

        with hook.get_conn().cursor() as cursor:

            def load(batch: list[dict], _format: str) -> None:
                try:
                    self.load_files(cursor, batch, _format, _temporary_partition_clause)
                except GeneCoverageFileError as e:
                    if len(batch) == 1:
                        fail(batch[0], e)
                        return
                    # The job wrote no row: load its files one by one, to keep the good ones.
                    self.log.warning(f"The load of {len(batch)} files failed, loading them one by one: {e}")
                    for file in batch:
                        load([file], _format)
                else:
                    loaded_task_ids.update(file["task_id"] for file in batch)

            for _format, format_files in files_by_format.items():
                for batch in itertools.batched(format_files, self.files_per_load):
                    load(list(batch), _format)

        # Swap all the same: the copy step has already dropped the rows of the experiments this run reloads.
        if partition_exists and self.is_swap:
            if failed_seq_ids:
                self.restore_failed_experiments(hook, failed_seq_ids, loaded_task_ids)
            self.swap_partitions()


def raise_gene_coverage_errors(errors: list[dict] | None) -> None:
    """Fail with the S3 path and the error of each `covgene` file that could not be loaded."""
    if not errors:
        return
    lines = "\n".join(f"- {e['filepath']} (task {e['task_id']}): {e['error']}" for e in errors)
    raise GeneCoverageLoadError(f"{len(errors)} coverage_by_gene file(s) could not be loaded:\n{lines}")
