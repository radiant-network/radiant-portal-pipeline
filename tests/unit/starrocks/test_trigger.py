import asyncio
from unittest.mock import MagicMock, patch

import pytest
from airflow.triggers.base import TriggerEvent
from pymysql import ProgrammingError

from radiant.tasks.starrocks.trigger import StarRocksTaskCompleteTrigger


@pytest.fixture
def mock_connection():
    with patch("radiant.tasks.starrocks.trigger.BaseHook.get_connection") as mock_get_connection:
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.get_hook.return_value.get_conn.return_value.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        yield mock_cursor


@pytest.fixture
def context():
    return {}


def test_get_task_completed_success(mock_connection):
    mock_connection.fetchone.return_value = ("SUCCESS", None)

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)
    result = trigger._get_task_completed(mock_connection)

    assert isinstance(result, TriggerEvent)
    assert result.payload.get("status") == "success"


def test_get_task_completed_failed(mock_connection):
    mock_connection.fetchone.return_value = ("FAILED", "fake_out_of_memory_error")

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)
    result = trigger._get_task_completed(mock_connection)

    assert isinstance(result, TriggerEvent)
    assert result.payload.get("error_message") == "fake_out_of_memory_error"
    assert result.payload.get("state") == "FAILED"


def test_get_task_completed_running(mock_connection):
    mock_connection.fetchone.return_value = ("RUNNING", None)

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)
    result = trigger._get_task_completed(mock_connection)

    assert result is None


def test_get_task_completed_none(mock_connection):
    mock_connection.fetchone.return_value = None

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)

    result = [trigger._get_task_completed(mock_connection) for _ in range(5)]

    assert result[:4] == [None, None, None, None]


def test_get_task_completed_unknown(mock_connection):
    mock_connection.fetchone.return_value = ("FOOBAR", None)

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)
    result = trigger._get_task_completed(mock_connection)

    assert isinstance(result, TriggerEvent)
    assert result.payload.get("error_message") is None
    assert result.payload.get("state") == "FOOBAR"


def test_run_trigger_until_success(mock_connection):
    mock_connection.fetchone.side_effect = [("RUNNING", None), ("SUCCESS", None)]

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=1)

    async def run_trigger():
        result = [event async for event in trigger.run()]
        return result

    result = asyncio.run(run_trigger())

    assert len(result) == 1
    assert isinstance(result[0], TriggerEvent)
    assert result[0].payload.get("status") == "success"


def test_run_trigger_until_failure(mock_connection):
    mock_connection.fetchone.side_effect = [
        ("RUNNING", None),
        ("FAILED", "fake_out_of_memory_error"),
    ]

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=1)

    async def run_trigger():
        result = [event async for event in trigger.run()]
        return result

    result = asyncio.run(run_trigger())

    assert len(result) == 1
    assert isinstance(result[0], TriggerEvent)
    assert result[0].payload.get("error_message") == "fake_out_of_memory_error"
    assert result[0].payload.get("state") == "FAILED"


def test_run_trigger_until_unknown(mock_connection):
    mock_connection.fetchone.side_effect = [None, None, None, None, None]

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=1)

    async def run_trigger():
        result = [event async for event in trigger.run()]
        return result

    result = asyncio.run(run_trigger())

    assert len(result) == 1
    assert isinstance(result[0], TriggerEvent)
    assert result[0].payload.get("error_message") == "Caught <class 'StopIteration'> caused test_task to fail"


def test_get_task_complete_programming_error(mock_connection):
    mock_connection.execute.side_effect = ProgrammingError()

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)

    result = [trigger._get_task_completed(mock_connection) for _ in range(5)]

    assert isinstance(result[-1], TriggerEvent)
    assert (
        result[-1].payload.get("error_message")
        == "Caught <class 'pymysql.err.ProgrammingError'> caused test_task to fail"
    )
