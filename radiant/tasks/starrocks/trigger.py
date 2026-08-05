import asyncio
import logging
import math
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

LOGGER = logging.getLogger(__name__)

DEFAULT_QUERY_TIMEOUT = 60

# StarRocks creates the `task_runs` row asynchronously after `SUBMIT TASK` returns, so the first polls can
# legitimately find nothing. Keep the grace window time-based rather than attempt-based, otherwise a short
# `poll_interval` silently shrinks it.
_NOT_FOUND_GRACE_SECONDS = 120


class StarRocksTaskCompleteTrigger(BaseTrigger):
    _MAX_QUERY_ERRORS = 5

    def __init__(self, conn_id, task_name, sleep_time, query_timeout: int = DEFAULT_QUERY_TIMEOUT):
        super().__init__()
        self.conn_id = conn_id
        self.task_name = task_name
        self._sleep_time = sleep_time
        self._query_timeout = query_timeout
        self._max_not_found = max(self._MAX_QUERY_ERRORS, math.ceil(_NOT_FOUND_GRACE_SECONDS / max(sleep_time, 1)))
        self._query_error_count = 0
        self._not_found_count = 0

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "radiant.tasks.starrocks.trigger.StarRocksTaskCompleteTrigger",
            {
                "conn_id": self.conn_id,
                "task_name": self.task_name,
                "sleep_time": self._sleep_time,
                "query_timeout": self._query_timeout,
            },
        )

    def _fetch_last_run(self) -> tuple | None:
        """Read the latest task run state for `self.task_name`.

        Blocking: opens a short-lived connection, queries, then closes it. Runs in a worker thread, never on
        the triggerer event loop. A fresh connection per poll re-resolves DNS and sidesteps half-open sockets
        left behind by an FE restart or a pod being rescheduled.
        """
        connection = BaseHook.get_connection(self.conn_id)
        hook = connection.get_hook(hook_params={})
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT state, error_message
                    FROM information_schema.task_runs
                    WHERE task_name = '{self.task_name}'
                    ORDER BY CREATE_TIME DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()
        finally:
            try:
                conn.close()
            except Exception as e:  # noqa: BLE001 - closing must never mask the query outcome
                LOGGER.debug(f"Ignoring error while closing connection: [{type(e)}] {e}")

    def _evaluate_state(self, result: tuple | None) -> TriggerEvent | None:
        """Map a `task_runs` row to a terminal event, or None to keep polling."""
        if not result:
            self._not_found_count += 1
            LOGGER.info(
                f"No task run found for {self.task_name} ({self._not_found_count}/{self._max_not_found}), retrying..."
            )
            if self._not_found_count >= self._max_not_found:
                return TriggerEvent({"status": "error", "error_message": f"task {self.task_name} not found"})
            return None

        self._query_error_count = 0
        self._not_found_count = 0

        if result[0] == "SUCCESS":
            return TriggerEvent({"status": "success"})
        if result[0] not in ["RUNNING", "PENDING"]:
            return TriggerEvent({"status": "error", "state": result[0], "error_message": result[1]})
        return None

    async def _poll_once(self) -> TriggerEvent | None:
        """Poll StarRocks once without ever blocking the triggerer event loop.

        `wait_for` bounds a hung socket so the loop can retry with a fresh connection. Note the abandoned
        thread stays blocked on `recv()` and holds a default-executor slot until the socket errors out — set
        `read_timeout`/`connect_timeout` in the `starrocks_conn` extra to bound that too (see README).
        """
        try:
            result = await asyncio.wait_for(asyncio.to_thread(self._fetch_last_run), timeout=self._query_timeout)
        except TimeoutError:
            return self._on_query_error(f"query timed out after {self._query_timeout}s")
        except Exception as e:  # noqa: BLE001 - any driver error is retryable
            return self._on_query_error(f"[{type(e)}] {e}")

        return self._evaluate_state(result)

    def _on_query_error(self, message: str) -> TriggerEvent | None:
        self._query_error_count += 1
        LOGGER.info(
            f"Retrying after receiving ({self._query_error_count}/{self._MAX_QUERY_ERRORS}): {message}",
        )
        if self._query_error_count >= self._MAX_QUERY_ERRORS:
            return TriggerEvent(
                {
                    "status": "error",
                    "error_message": f"Polling {self.task_name} failed {self._MAX_QUERY_ERRORS} times: {message}",
                }
            )
        return None

    async def run(self):
        result = await self._poll_once()
        while result is None:
            await asyncio.sleep(self._sleep_time)
            result = await self._poll_once()
        yield result
