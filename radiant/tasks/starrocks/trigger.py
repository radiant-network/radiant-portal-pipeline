import asyncio
import logging
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

LOGGER = logging.getLogger(__name__)


class StarRocksTaskCompleteTrigger(BaseTrigger):
    _MISSED_MAX_COUNT = 5

    def __init__(self, conn_id, task_name, sleep_time):
        super().__init__()
        self.conn_id = conn_id
        self.task_name = task_name
        self._sleep_time = sleep_time
        self._missed_count = 0

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "radiant.tasks.starrocks.trigger.StarRocksTaskCompleteTrigger",
            {
                "conn_id": self.conn_id,
                "task_name": self.task_name,
                "sleep_time": self._sleep_time,
            },
        )

    def _get_task_completed(self, cursor):
        try:
            cursor.execute(
                f"""
                SELECT state, error_message
                FROM information_schema.task_runs
                WHERE task_name = '{self.task_name}'
                ORDER BY CREATE_TIME DESC
                LIMIT 1
                """
            )
            result = cursor.fetchone()
        except Exception as e:
            LOGGER.info(f"Retrying after receiving: [{type(e)}] {e}")
            self._missed_count += 1
            if self._missed_count >= self._MISSED_MAX_COUNT:
                return TriggerEvent({"error_message": f"Caught {type(e)} caused {self.task_name} to fail"})
            return None

        if not result:
            LOGGER.info("Expected variable `result` is None, retrying...")
            self._missed_count += 1
            if self._missed_count >= self._MISSED_MAX_COUNT:
                TriggerEvent({"status": "error", "error_message": f"task {self.task_name} not found"})
            return None

        self._missed_count = 0

        if result[0] == "SUCCESS":
            return TriggerEvent({"status": "success"})
        if result[0] not in ["RUNNING", "PENDING"]:
            return TriggerEvent({"status": "error", "state": result[0], "error_message": result[1]})
        return None

    async def run(self):
        connection = BaseHook.get_connection(self.conn_id)
        hook = connection.get_hook(hook_params={})
        conn = hook.get_conn()
        cursor = conn.cursor()

        result = self._get_task_completed(cursor)
        while result is None:
            await asyncio.sleep(self._sleep_time)
            result = self._get_task_completed(cursor)
        yield result
