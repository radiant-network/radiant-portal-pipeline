import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TaskFailedEvent, TaskSuccessEvent
from pymysql.err import ProgrammingError

LOGGER = logging.getLogger(__name__)


class StarRocksTaskCompleteTrigger(BaseTrigger):
    """
    Trigger to check the completion status of a StarRocks task.

    Args:
        conn_id (str): Connection ID for the StarRocks database.
        task_name (str): Name of the task to check.
        sleep_time (int): Time in seconds to wait between checks.
    """

    _MISSED_MAX_COUNT = 5

    def __init__(self, conn_id, task_name, sleep_time):
        """
        Initialize the trigger with connection ID, task name, and sleep time.

        Args:
            conn_id (str): Connection ID for the StarRocks database.
            task_name (str): Name of the task to check.
            sleep_time (int): Time in seconds to wait between checks.
        """
        super().__init__()
        self.conn_id = conn_id
        self.task_name = task_name
        self._sleep_time = sleep_time

        connection = BaseHook.get_connection(conn_id)
        self.cursor = connection.get_hook(hook_params={}).get_conn().cursor()
        self._missed_count = 0

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize the trigger for Airflow to use.

        Returns:
            tuple[str, dict[str, Any]]: Serialized trigger information.
        """
        return (
            "commons.triggers.starrocks.StarRocksTaskCompleteTrigger",
            {
                "conn_id": self.conn_id,
                "task_name": self.task_name,
                "sleep_time": self._sleep_time,
            },
        )

    def _get_task_completed(self):
        """
        Check the completion status of the task.

        Returns:
            TaskFailedEvent or TaskSuccessEvent: Event indicating task success or failure.
        """
        try:
            self.cursor.execute(
                f"""
                SELECT state, error_message
                FROM information_schema.task_runs
                WHERE task_name = '{self.task_name}'
                ORDER BY CREATE_TIME DESC
                LIMIT 1
                """
            )
        except ProgrammingError as pe:
            LOGGER.info(f"Retrying after receiving: {pe}")
            self._missed_count += 1
            if self._missed_count == self._MISSED_MAX_COUNT:
                return TaskFailedEvent(xcoms={"error_message": f"ProgrammingErrors caused {self.task_name} to fail"})
            return None

        result = self.cursor.fetchone()
        if not result:
            LOGGER.info("Expected variable `result` is None, retrying...")
            self._missed_count += 1
            if self._missed_count == self._MISSED_MAX_COUNT:
                return TaskFailedEvent(xcoms={"error_message": f"task {self.task_name} not found"})
            return None

        self._missed_count = 0

        if result[0] == "SUCCESS":
            return TaskSuccessEvent()

        if result[0] not in ["RUNNING", "PENDING"]:
            LOGGER.info(f"Received task state {result[0]}, failing task...")
            return TaskFailedEvent(xcoms={"error_message": f"state: {result[0]}, error_message: {result[1]}"})

        return None

    async def run(self) -> AsyncIterator[TaskSuccessEvent]:
        """
        Run the trigger to check task completion status periodically.

        Yields:
            TaskSuccessEvent: Event indicating task success.
        """
        result = self._get_task_completed()
        while result is None:
            await asyncio.sleep(self._sleep_time)
            result = self._get_task_completed()

        yield result
