import asyncio
from typing import Any, AsyncIterator

from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TaskFailedEvent, TaskSuccessEvent


class StarRocksTaskCompleteTrigger(BaseTrigger):

    def __init__(self, conn_id, task_name, sleep_time):
        super().__init__()
        self.conn_id = conn_id
        self.task_name = task_name
        self._sleep_time = sleep_time

        connection = BaseHook.get_connection(conn_id)
        self.cursor = connection.get_hook(hook_params={}).get_conn().cursor()

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "lib.triggers.starrocks.StarRocksTaskCompleteTrigger",
            {
                "conn_id": self.conn_id,
                "task_name": self.task_name,
                "sleep_time": self._sleep_time,
            },
        )

    def _get_task_completed(self):
        self.cursor.execute(
            f"""
            SELECT state, error_message
            FROM information_schema.task_runs
            WHERE task_name = '{self.task_name}'
            ORDER BY CREATE_TIME DESC
            LIMIT 1
            """
        )
        result = self.cursor.fetchone()
        if not result or result[0] == "FAILED":
            return TaskFailedEvent(xcoms={"error_message": result[1]})

        if result[0] != "RUNNING":
            return TaskSuccessEvent()

        return None

    async def run(self) -> AsyncIterator[TaskSuccessEvent]:
        result = self._get_task_completed()
        while result is None:
            await asyncio.sleep(self._sleep_time)
            result = self._get_task_completed()

        yield result
