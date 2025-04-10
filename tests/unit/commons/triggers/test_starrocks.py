import asyncio
from unittest.mock import MagicMock, patch

import pytest
from airflow.triggers.base import TaskFailedEvent, TaskSuccessEvent
from pymysql import ProgrammingError

from dags.commons.triggers.starrocks import StarRocksTaskCompleteTrigger


@pytest.fixture
def mock_connection():
    with patch("dags.commons.triggers.starrocks.BaseHook.get_connection") as mock_get_connection:
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
    result = trigger._get_task_completed()

    assert isinstance(result, TaskSuccessEvent)


def test_get_task_completed_failed(mock_connection):
    mock_connection.fetchone.return_value = ("FAILED", "fake_out_of_memory_error")

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)
    result = trigger._get_task_completed()

    assert isinstance(result, TaskFailedEvent)
    assert result.xcoms["error_message"] == "state: FAILED, error_message: fake_out_of_memory_error"


def test_get_task_completed_running(mock_connection):
    mock_connection.fetchone.return_value = ("RUNNING", None)

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)
    result = trigger._get_task_completed()

    assert result is None


def test_get_task_completed_none(mock_connection):
    mock_connection.fetchone.return_value = None

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)

    result = [trigger._get_task_completed() for _ in range(5)]

    assert result[:4] == [None, None, None, None]
    assert isinstance(result[4], TaskFailedEvent)
    assert result[4].xcoms["error_message"] == "task test_task not found"


def test_get_task_completed_unknown(mock_connection):
    mock_connection.fetchone.return_value = ("FOOBAR", None)

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)
    result = trigger._get_task_completed()

    assert isinstance(result, TaskFailedEvent)
    assert result.xcoms["error_message"] == "state: FOOBAR, error_message: None"


def test_run_trigger_until_success(mock_connection):
    mock_connection.fetchone.side_effect = [("RUNNING", None), ("SUCCESS", None)]

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=1)

    async def run_trigger():
        result = [event async for event in trigger.run()]
        return result

    result = asyncio.run(run_trigger())

    assert len(result) == 1
    assert isinstance(result[0], TaskSuccessEvent)


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
    assert isinstance(result[0], TaskFailedEvent)
    assert result[0].xcoms["error_message"] == "state: FAILED, error_message: fake_out_of_memory_error"


def test_run_trigger_until_unknown(mock_connection):
    mock_connection.fetchone.side_effect = [None, None, None, None, None]

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=1)

    async def run_trigger():
        result = [event async for event in trigger.run()]
        return result

    result = asyncio.run(run_trigger())

    assert len(result) == 1
    assert isinstance(result[0], TaskFailedEvent)
    assert result[0].xcoms["error_message"] == "task test_task not found"


def test_get_task_complete_programming_error(mock_connection):
    mock_connection.execute.side_effect = ProgrammingError()

    trigger = StarRocksTaskCompleteTrigger(conn_id="test_conn", task_name="test_task", sleep_time=5)

    result = [trigger._get_task_completed() for _ in range(5)]

    assert isinstance(result[-1], TaskFailedEvent)
    assert (
        result[-1].xcoms["error_message"] == "Caught <class 'pymysql.err.ProgrammingError'> caused test_task to fail"
    )
