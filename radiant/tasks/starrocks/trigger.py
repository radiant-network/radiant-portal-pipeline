import asyncio
import logging
import math
from collections.abc import Callable
from functools import partial
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

LOGGER = logging.getLogger(__name__)

DEFAULT_QUERY_TIMEOUT = 60

# StarRocks creates the `task_runs` row asynchronously after `SUBMIT TASK` returns, so the first polls can
# legitimately find nothing. Keep the grace window time-based rather than attempt-based, otherwise a short
# `poll_interval` silently shrinks it.
_NOT_FOUND_GRACE_SECONDS = 120

# `starrocks_conn` extras forwarded to `MySQLdb.connect` (mysqlclient accepts all three, `_mysql.c`).
_DRIVER_TIMEOUTS = ("connect_timeout", "read_timeout", "write_timeout")


class UnsupportedConnectionError(TypeError):
    """The trigger's connection is not a MySqlHook one; retrying cannot fix it."""


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

    async def _aget_connect(self) -> Callable[[], Any]:
        """Resolve everything Airflow-side on the event loop; return a plain driver `connect` for the thread.

        Rule: a worker thread must never call into Airflow. On Airflow 3 any supervisor call from a thread
        (`get_connection`, or the `MaskSecret` each `Connection.extra_dejson` read sends) runs on a second
        event loop over the runner's socket, and the runner dies on "Response read out of order!". Airflow 2
        has no `aget_connection` and no supervisor, so a thread is fine there. `get_hook` scans provider entry
        points on its first call, a one-off cost on the loop.
        """
        aget_connection = getattr(BaseHook, "aget_connection", None)
        if aget_connection is not None:
            connection = await aget_connection(self.conn_id)
        else:
            connection = await asyncio.to_thread(BaseHook.get_connection, self.conn_id)
        hook = connection.get_hook(hook_params={"connection": connection})
        return self._driver_connect(hook, connection)

    @staticmethod
    def _driver_connect(hook, connection) -> Callable[[], Any]:
        """The driver call `MySqlHook.get_conn` would make, minus its Airflow lookups (same kwargs builders).

        mysqlclient also gets the `_DRIVER_TIMEOUTS` from the extra: the provider (6.5.2) drops them, and they
        are what releases a thread `wait_for` abandoned in `recv()`.
        """
        extra = connection.extra_dejson
        if extra.get("client", "mysqlclient") == "mysql-connector-python":
            builder_name = "_get_conn_config_mysql_connector_python"
        else:
            builder_name = "_get_conn_config_mysql_client"
        builder = getattr(hook, builder_name, None)
        if builder is None:
            raise UnsupportedConnectionError(
                f"Connection {connection.conn_id!r} resolves to {type(hook).__name__}, which has no "
                f"{builder_name}: StarRocksTaskCompleteTrigger needs a `mysql` connection (MySqlHook)."
            )

        if builder_name == "_get_conn_config_mysql_connector_python":
            import mysql.connector

            return partial(mysql.connector.connect, **builder(connection))
        import MySQLdb

        timeouts = {key: extra[key] for key in _DRIVER_TIMEOUTS if key in extra}
        return partial(MySQLdb.connect, **builder(connection), **timeouts)

    def _fetch_last_run(self, connect: Callable[[], Any]) -> tuple | None:
        """Read the latest task run state for `self.task_name`.

        Blocking: opens a short-lived connection, queries, then closes it. Runs in a worker thread, never on
        the triggerer event loop. A fresh connection per poll re-resolves DNS and sidesteps half-open sockets
        left behind by an FE restart or a pod being rescheduled. `connect` is a bare driver call: nothing in
        this thread may call into Airflow (see `_aget_connect`).
        """
        conn = connect()
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

        `_aget_connect` stays outside `wait_for`: cancelling it between its supervisor request and the reply
        would leave the reply on the socket for the runner's next read, which is the very desync it avoids.
        A stalled supervisor is bounded by the operator's defer timeout instead.
        """
        try:
            connect = await self._aget_connect()
        except UnsupportedConnectionError as e:
            return TriggerEvent({"status": "error", "error_message": str(e)})
        except Exception as e:  # noqa: BLE001 - a supervisor/secrets hiccup is retryable
            return self._on_query_error(f"[{type(e)}] {e}")

        try:
            result = await asyncio.wait_for(asyncio.to_thread(self._fetch_last_run, connect), self._query_timeout)
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
