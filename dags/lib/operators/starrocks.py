import uuid

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import TaskSuccessEvent

from ..triggers.starrocks import (
    StarRocksTaskCompleteTrigger,
)

STARROCKS_TASK_TEMPLATE = "StarRocksSQLExecuteQueryOperator_Task_{uid}"


class StarRocksSQLExecuteQueryOperator(SQLExecuteQueryOperator, BaseSensorOperator):

    def __init__(
        self,
        submit_task: bool = False,
        max_query_timeout: int = 10000,
        poll_interval: int = 30,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.submit_task = submit_task
        self._query_timeout = max_query_timeout
        self.poll_interval = poll_interval

    @staticmethod
    def _prepare_sql(sql: str, query_timeout: int) -> tuple[str, str]:
        _task_name = STARROCKS_TASK_TEMPLATE.format(uid=str(uuid.uuid4())[-8:])
        return (
            f"""
            submit /*+set_var(query_timeout={query_timeout})*/ task {_task_name} as
            {sql}
         """,
            _task_name,
        )

    def execute(self, context):
        if self.submit_task:
            self.sql, _task_name = self._prepare_sql(
                sql=self.sql, query_timeout=self._query_timeout
            )

            super().execute(context)
            self.defer(
                trigger=StarRocksTaskCompleteTrigger(
                    conn_id=self.conn_id,
                    task_name=_task_name,
                    sleep_time=self.poll_interval,
                ),
                method_name="_is_complete",
            )
        else:
            return super().execute(context)

    def _is_complete(self, context, event=None) -> None:
        if not isinstance(event, TaskSuccessEvent):
            print("Task failed")
        return
