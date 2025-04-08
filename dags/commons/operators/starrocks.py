import uuid
from dataclasses import dataclass
from typing import Any

from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.triggers.base import TaskSuccessEvent

from ..triggers.starrocks import (
    StarRocksTaskCompleteTrigger,
)

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
    enable_spill: bool = False
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
        submit_task: bool = False,
        submit_task_options: SubmitTaskOptions = None,
        query_params: dict = None,
        **kwargs,
    ):
        conn_id = Variable.get("STARROCKS_CONNECTION_ID")
        database = Variable.get("STARROCKS_DATABASE")

        super().__init__(
            conn_id=conn_id,
            database=database,
            **kwargs,
        )
        self.submit_task = submit_task
        self.submit_task_options = submit_task_options or SubmitTaskOptions()
        self.query_params = query_params or {}

    @staticmethod
    def _prepare_sql(
        sql: str,
        is_submit_task: bool,
        query_timeout: int,
        enable_spill: bool = False,
        spill_mode: str = "auto",
        query_params: dict = None,
        extra_args: dict = None,
    ) -> tuple[str, str]:
        """
        Prepare the SQL query with the given parameters.

        Args:
            sql (str): The SQL query to be executed.
            query_timeout (int): Maximum query timeout in milliseconds.
            enable_spill (bool): Flag to enable or disable spilling.
            spill_mode (str): Mode of spilling, e.g., 'auto'.
            query_params (dict): Parameters to format the SQL query.

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
                _extra_args = ", ".join(
                    [f"{key}={value}" for key, value in extra_args.items()]
                )
                _submit_config += f", {_extra_args}"

            _sql = f"""
                submit /*+set_var(
                {_submit_config}
                )*/ task {_task_name} as
                {sql}
             """
        else:
            _sql = sql

        if query_params:
            _sql = (
                _sql.format(**{key: value for key, value in query_params.items()})
                if query_params
                else _sql
            )

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
            query_params=self.query_params,
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
            event (TaskSuccessEvent): The event indicating task success.
        """
        if not isinstance(event, TaskSuccessEvent):
            print("Task failed")
        return
