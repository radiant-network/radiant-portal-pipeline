import uuid
from dataclasses import dataclass

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import TaskSuccessEvent

from ..triggers.starrocks import (
    StarRocksTaskCompleteTrigger,
)

STARROCKS_TASK_TEMPLATE = "StarRocksSQLExecuteQueryOperator_Task_{uid}"


@dataclass
class SubmitTaskOptions:
    max_query_timeout: int = 10000
    poll_interval: int = 30
    enable_spill: bool = False
    spill_mode: str = "auto"


class StarRocksSQLExecuteQueryOperator(SQLExecuteQueryOperator, BaseSensorOperator):

    def __init__(
        self,
        submit_task: bool = False,
        submit_task_options: SubmitTaskOptions = SubmitTaskOptions(),
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.submit_task = submit_task
        self.submit_task_options = submit_task_options

    @staticmethod
    def _prepare_sql(
        sql: str,
        query_timeout: int,
        enable_spill: bool = False,
        spill_mode: str = "auto",
    ) -> tuple[str, str]:
        _task_name = STARROCKS_TASK_TEMPLATE.format(uid=str(uuid.uuid4())[-8:])
        return (
            f"""
            submit /*+set_var(
            query_timeout={query_timeout}, 
            enable_spill={enable_spill}, 
            spill_mode={spill_mode}
            )*/ task {_task_name} as
            {sql}
         """,
            _task_name,
        )

    def execute(self, context):
        if self.submit_task:
            self.sql, _task_name = self._prepare_sql(
                sql=self.sql,
                query_timeout=self.submit_task_options.max_query_timeout,
                enable_spill=self.submit_task_options.enable_spill,
                spill_mode=self.submit_task_options.spill_mode,
            )

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
        if not isinstance(event, TaskSuccessEvent):
            print("Task failed")
        return
