import asyncio
from typing import Any, AsyncIterator

from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TaskFailedEvent, TaskSuccessEvent


class StarRocksTaskCompleteTrigger(BaseTrigger):
    """
    Trigger to check the completion status of a StarRocks task.

    Args:
        conn_id (str): Connection ID for the StarRocks database.
        task_name (str): Name of the task to check.
        sleep_time (int): Time in seconds to wait between checks.
    """

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
        self.cursor.execute(
            f"""
            SELECT state, error_message
            FROM information_schema.task_runs
            WHERE task_name = '{self.task_name}'
            ORDER BY CREATE_TIME DESC
            LIMIT 1
            """
        )
        state, error_msg = self.cursor.fetchone()

        if not state:
            return TaskFailedEvent(xcoms={"error_message": "empty result set"})

        elif state == "FAILED":
            return TaskFailedEvent(xcoms={"error_message": error_msg})

        elif state == "SUCCESS":
            return TaskSuccessEvent()

        elif state != "RUNNING":
            return TaskFailedEvent(xcoms={"error_message": f"unknown state: {error_msg}"})

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
