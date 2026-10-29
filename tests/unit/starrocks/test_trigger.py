import asyncio
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from airflow.triggers.base import TriggerEvent
from pymysql import ProgrammingError

from radiant.tasks.starrocks.trigger import StarRocksTaskCompleteTrigger, UnsupportedConnectionError


class _StarRocksMock:
    """Handles on the mocked connection chain, so tests can assert reconnect/close behaviour."""

    def __init__(self, aget_connection, connection, conn, cursor):
        self.aget_connection = aget_connection
        self.connection = connection
        self.conn = conn
        self.cursor = cursor

    @property
    def poll_count(self) -> int:
        """How many times the trigger opened a fresh connection."""
        return self.aget_connection.await_count


@pytest.fixture
def starrocks():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connection = MagicMock()
    with (
        patch("radiant.tasks.starrocks.trigger.BaseHook.aget_connection", new_callable=AsyncMock) as mock_aget,
        patch.object(StarRocksTaskCompleteTrigger, "_driver_connect", return_value=lambda: mock_conn),
    ):
        mock_aget.return_value = mock_connection
        yield _StarRocksMock(mock_aget, mock_connection, mock_conn, mock_cursor)


@pytest.fixture(autouse=True)
def no_wait():
    """Skip the inter-poll backoff so tests exercise the loop, not the wall clock."""

    async def _noop(_delay):
        return None

    with patch("radiant.tasks.starrocks.trigger.asyncio.sleep", _noop):
        yield


def _trigger(sleep_time=1, **kwargs):
    return StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=sleep_time, **kwargs)


async def _aget_connect():
    return MagicMock()


def _run(trigger):
    async def run_trigger():
        return [event async for event in trigger.run()]

    return asyncio.run(run_trigger())


def test_evaluate_state_success():
    result = _trigger()._evaluate_state(("SUCCESS", None))

    assert isinstance(result, TriggerEvent)
    assert result.payload.get("status") == "success"


def test_evaluate_state_failed():
    result = _trigger()._evaluate_state(("FAILED", "fake_out_of_memory_error"))

    assert isinstance(result, TriggerEvent)
    assert result.payload.get("status") == "error"
    assert result.payload.get("error_message") == "fake_out_of_memory_error"
    assert result.payload.get("state") == "FAILED"


@pytest.mark.parametrize("state", ["RUNNING", "PENDING"])
def test_evaluate_state_pending_keeps_polling(state):
    assert _trigger()._evaluate_state((state, None)) is None


def test_evaluate_state_unknown():
    result = _trigger()._evaluate_state(("FOOBAR", None))

    assert isinstance(result, TriggerEvent)
    assert result.payload.get("error_message") is None
    assert result.payload.get("state") == "FOOBAR"


def test_evaluate_state_not_found_exhausts_grace():
    # sleep_time=30 -> ceil(120/30) = 4, floored to _MAX_QUERY_ERRORS (5).
    trigger = _trigger(sleep_time=30)

    result = [trigger._evaluate_state(None) for _ in range(5)]

    assert result[:4] == [None, None, None, None]
    assert isinstance(result[4], TriggerEvent)
    assert result[4].payload.get("status") == "error"
    assert result[4].payload.get("error_message") == "task test_task not found"


def test_not_found_grace_does_not_shrink_with_short_poll_interval():
    # A 10s poll interval must still allow ~120s of "row not visible yet", not 5 attempts (50s).
    trigger = _trigger(sleep_time=10)

    assert trigger._max_not_found == 12
    assert [trigger._evaluate_state(None) for _ in range(11)] == [None] * 11
    assert isinstance(trigger._evaluate_state(None), TriggerEvent)


def test_evaluate_state_resets_counters_after_transient_miss():
    trigger = _trigger(sleep_time=30)

    assert trigger._evaluate_state(None) is None
    assert trigger._evaluate_state(("RUNNING", None)) is None
    assert trigger._not_found_count == 0


def test_run_trigger_until_success(starrocks):
    starrocks.cursor.fetchone.side_effect = [("RUNNING", None), ("SUCCESS", None)]

    result = _run(_trigger())

    assert len(result) == 1
    assert result[0].payload.get("status") == "success"


def test_run_trigger_until_failure(starrocks):
    starrocks.cursor.fetchone.side_effect = [
        ("RUNNING", None),
        ("FAILED", "fake_out_of_memory_error"),
    ]

    result = _run(_trigger())

    assert len(result) == 1
    assert result[0].payload.get("error_message") == "fake_out_of_memory_error"
    assert result[0].payload.get("state") == "FAILED"


def test_run_trigger_until_unknown(starrocks):
    starrocks.cursor.fetchone.side_effect = [None] * 5

    result = _run(_trigger(sleep_time=30))

    assert len(result) == 1
    assert result[0].payload.get("status") == "error"
    assert result[0].payload.get("error_message") == "task test_task not found"


def test_run_reconnects_on_every_poll(starrocks):
    starrocks.cursor.fetchone.side_effect = [("RUNNING", None), ("RUNNING", None), ("SUCCESS", None)]

    _run(_trigger())

    # A stale connection can no longer wedge the loop: each poll dials StarRocks again and hangs up after.
    assert starrocks.poll_count == 3
    assert starrocks.conn.close.call_count == 3


def test_run_closes_connection_even_when_query_fails(starrocks):
    starrocks.cursor.execute.side_effect = [ProgrammingError(), None]
    starrocks.cursor.fetchone.return_value = ("SUCCESS", None)

    result = _run(_trigger())

    assert result[0].payload.get("status") == "success"
    assert starrocks.conn.close.call_count == 2


def test_run_recovers_from_query_error(starrocks):
    starrocks.cursor.execute.side_effect = [ProgrammingError(), None]
    starrocks.cursor.fetchone.return_value = ("SUCCESS", None)

    trigger = _trigger()
    result = _run(trigger)

    assert result[0].payload.get("status") == "success"
    assert trigger._query_error_count == 0


def test_run_fails_after_repeated_query_errors(starrocks):
    starrocks.cursor.execute.side_effect = ProgrammingError()

    result = _run(_trigger())

    assert len(result) == 1
    assert result[0].payload.get("status") == "error"
    assert "failed 5 times" in result[0].payload.get("error_message")
    assert "ProgrammingError" in result[0].payload.get("error_message")


def test_run_recovers_from_hung_query():
    """A hung socket must be bounded by the query timeout instead of blocking the event loop forever."""
    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.Event().wait()  # never set; stands in for a blocked recv()
        return ("SUCCESS", None)

    # `new=` (not side_effect): patch would otherwise build an AsyncMock for the async `to_thread` and the
    # inner coroutine would come back un-awaited.
    trigger = _trigger(query_timeout=0.05)
    with (
        patch.object(trigger, "_aget_connect", new=_aget_connect),
        patch("radiant.tasks.starrocks.trigger.asyncio.to_thread", new=lambda _fn, _conn: fetch()),
    ):
        result = _run(trigger)

    assert len(result) == 1
    assert result[0].payload.get("status") == "success"
    assert calls["n"] == 2


def test_run_fails_after_repeated_hung_queries():
    async def never_returns():
        await asyncio.Event().wait()

    trigger = _trigger(query_timeout=0.01)
    with (
        patch.object(trigger, "_aget_connect", new=_aget_connect),
        patch("radiant.tasks.starrocks.trigger.asyncio.to_thread", new=lambda _fn, _conn: never_returns()),
    ):
        result = _run(trigger)

    assert result[0].payload.get("status") == "error"
    assert "query timed out" in result[0].payload.get("error_message")


def test_airflow_is_only_touched_on_the_event_loop():
    """Airflow 3: any supervisor call from the worker thread (get_connection, or the MaskSecret sent by
    `extra_dejson`) corrupts the triggerer's supervisor channel. Only the bare driver call may run there."""
    threads = {}
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value.fetchone.return_value = ("SUCCESS", None)
    connection = MagicMock()

    async def aget_connection(conn_id):
        threads["aget_connection"] = threading.get_ident()
        return connection

    def driver_connect(hook, conn):
        threads["driver_connect"] = threading.get_ident()

        def connect():
            threads["connect"] = threading.get_ident()
            return mock_conn

        return connect

    async def run_trigger():
        threads["loop"] = threading.get_ident()
        return [event async for event in _trigger().run()]

    with (
        patch("radiant.tasks.starrocks.trigger.BaseHook.aget_connection", new=aget_connection),
        patch("radiant.tasks.starrocks.trigger.BaseHook.get_connection") as get_connection,
        patch.object(StarRocksTaskCompleteTrigger, "_driver_connect", new=staticmethod(driver_connect)),
    ):
        result = asyncio.run(run_trigger())

    assert result[0].payload.get("status") == "success"
    get_connection.assert_not_called()
    connection.get_hook.assert_called_once_with(hook_params={"connection": connection})
    assert threads["aget_connection"] == threads["loop"]
    assert threads["driver_connect"] == threads["loop"]
    assert threads["connect"] != threads["loop"]


def test_connection_lookup_is_not_cancelled_by_query_timeout(starrocks):
    """Cancelling a supervisor call between request and reply would desync the socket: never bound it."""
    starrocks.cursor.fetchone.return_value = ("SUCCESS", None)

    async def slow_aget_connection(conn_id):
        await asyncio.to_thread(time.sleep, 0.05)  # longer than query_timeout; asyncio.sleep is patched out
        return starrocks.connection

    with patch("radiant.tasks.starrocks.trigger.BaseHook.aget_connection", new=slow_aget_connection):
        result = _run(_trigger(query_timeout=0.01))

    assert result[0].payload.get("status") == "success"


def test_connection_lookup_error_is_retried(starrocks):
    starrocks.cursor.fetchone.return_value = ("SUCCESS", None)
    starrocks.aget_connection.side_effect = [RuntimeError("supervisor hiccup"), starrocks.connection]

    trigger = _trigger()
    result = _run(trigger)

    assert result[0].payload.get("status") == "success"
    assert starrocks.aget_connection.await_count == 2


def test_unsupported_connection_fails_without_retrying():
    with (
        patch("radiant.tasks.starrocks.trigger.BaseHook.aget_connection", new_callable=AsyncMock) as aget_connection,
        patch.object(
            StarRocksTaskCompleteTrigger, "_driver_connect", side_effect=UnsupportedConnectionError("not mysql")
        ),
    ):
        result = _run(_trigger())

    assert result[0].payload == {"status": "error", "error_message": "not mysql"}
    assert aget_connection.await_count == 1


_HOOK_KWARGS = {"host": "starrocks.mock", "port": 9030}
_TIMEOUTS = {"connect_timeout": 10, "read_timeout": 60, "write_timeout": 60}


@pytest.mark.parametrize(
    ("extra", "module", "builder", "expected_kwargs"),
    [
        ({}, "MySQLdb", "_get_conn_config_mysql_client", _HOOK_KWARGS),
        ({"client": "mysqlclient"}, "MySQLdb", "_get_conn_config_mysql_client", _HOOK_KWARGS),
        # The provider drops these extras; the trigger forwards them so an abandoned thread's recv() is bounded.
        (_TIMEOUTS, "MySQLdb", "_get_conn_config_mysql_client", {**_HOOK_KWARGS, **_TIMEOUTS}),
        (
            {"client": "mysql-connector-python", **_TIMEOUTS},
            "mysql.connector",
            "_get_conn_config_mysql_connector_python",
            _HOOK_KWARGS,
        ),
    ],
)
def test_driver_connect_uses_the_hook_kwargs_for_the_configured_client(extra, module, builder, expected_kwargs):
    hook = MagicMock()
    getattr(hook, builder).return_value = dict(_HOOK_KWARGS)
    connection = MagicMock(extra_dejson=extra)
    driver = MagicMock()
    modules = {module: driver, "mysql": MagicMock(connector=driver)} if "." in module else {module: driver}

    with patch.dict("sys.modules", modules):
        connect = StarRocksTaskCompleteTrigger._driver_connect(hook, connection)
        driver.connect.assert_not_called()
        connect()

    getattr(hook, builder).assert_called_once_with(connection)
    driver.connect.assert_called_once_with(**expected_kwargs)


def test_driver_connect_rejects_a_non_mysql_hook():
    connection = MagicMock(extra_dejson={}, conn_id="starrocks_conn")

    with pytest.raises(UnsupportedConnectionError, match="starrocks_conn.*MySqlHook"):
        StarRocksTaskCompleteTrigger._driver_connect(object(), connection)


def test_serialize_round_trips_query_timeout():
    classpath, kwargs = _trigger(sleep_time=7, query_timeout=42).serialize()

    assert classpath == "radiant.tasks.starrocks.trigger.StarRocksTaskCompleteTrigger"
    assert kwargs == {"conn_id": "test_conn", "task_name": "test_task", "sleep_time": 7, "query_timeout": 42}
    # Reassignment to another triggerer rebuilds the trigger from these kwargs.
    assert StarRocksTaskCompleteTrigger(**kwargs)._query_timeout == 42
