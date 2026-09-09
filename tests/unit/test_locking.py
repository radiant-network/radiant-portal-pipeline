import datetime
import io
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from radiant.tasks.locking import (
    LockHeldError,
    LockStatus,
    acquire_lock,
    check_lock,
    describe_lock_status,
    release_lock,
)


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code}}, "operation")


def test_acquire_lock_succeeds_when_key_absent():
    mock_client = MagicMock()

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        acquire_lock(bucket="warehouse", name="import_mutex", holder="dag:run-1")

    mock_client.put_object.assert_called_once_with(
        Bucket="warehouse", Key="_locks/import_mutex", Body=b"dag:run-1", IfNoneMatch="*"
    )
    mock_client.head_object.assert_not_called()
    mock_client.delete_object.assert_not_called()


def test_acquire_lock_raises_when_already_held():
    mock_client = MagicMock()
    mock_client.put_object.side_effect = _client_error("412")

    with (
        patch("radiant.tasks.locking.boto3.client", return_value=mock_client),
        pytest.raises(LockHeldError),
    ):
        acquire_lock(bucket="warehouse", name="import_mutex", holder="dag:run-1")

    mock_client.delete_object.assert_not_called()


def test_acquire_lock_raises_even_when_existing_lock_is_stale():
    # acquire_lock never deletes anything, however old the existing lock is -- clearing an
    # abandoned lock is a separate, deliberate action (the toolbox DAG's check-lock command).
    mock_client = MagicMock()
    mock_client.put_object.side_effect = _client_error("412")

    with (
        patch("radiant.tasks.locking.boto3.client", return_value=mock_client),
        pytest.raises(LockHeldError),
    ):
        acquire_lock(bucket="warehouse", name="import_mutex", holder="dag:run-2")

    mock_client.head_object.assert_not_called()
    mock_client.delete_object.assert_not_called()


def test_acquire_lock_reraises_unexpected_put_error():
    mock_client = MagicMock()
    mock_client.put_object.side_effect = _client_error("500")

    with (
        patch("radiant.tasks.locking.boto3.client", return_value=mock_client),
        pytest.raises(ClientError),
    ):
        acquire_lock(bucket="warehouse", name="import_mutex", holder="dag:run-1")


def test_release_lock_deletes_the_key():
    mock_client = MagicMock()

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        release_lock(bucket="warehouse", name="import_mutex")

    mock_client.delete_object.assert_called_once_with(Bucket="warehouse", Key="_locks/import_mutex")


def test_check_lock_reports_not_held_when_key_absent():
    mock_client = MagicMock()
    mock_client.get_object.side_effect = _client_error("404")

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        status = check_lock(bucket="warehouse", name="import_mutex")

    assert status.held is False
    assert status.holder is None
    assert status.expired is False


def test_check_lock_reports_fresh_lock():
    mock_client = MagicMock()
    fresh_time = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
    mock_client.get_object.return_value = {"Body": io.BytesIO(b"dag:run-1"), "LastModified": fresh_time}

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        status = check_lock(bucket="warehouse", name="import_mutex")

    assert status.held is True
    assert status.holder == "dag:run-1"
    assert status.expired is False
    assert status.age < datetime.timedelta(hours=1)


def test_check_lock_reports_expired_lock():
    mock_client = MagicMock()
    stale_time = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=7)
    mock_client.get_object.return_value = {"Body": io.BytesIO(b"dag:run-2"), "LastModified": stale_time}

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        status = check_lock(bucket="warehouse", name="import_mutex")

    assert status.held is True
    assert status.holder == "dag:run-2"
    assert status.expired is True


def test_check_lock_reraises_unexpected_error():
    mock_client = MagicMock()
    mock_client.get_object.side_effect = _client_error("500")

    with (
        patch("radiant.tasks.locking.boto3.client", return_value=mock_client),
        pytest.raises(ClientError),
    ):
        check_lock(bucket="warehouse", name="import_mutex")

    mock_client.delete_object.assert_not_called()


def test_describe_lock_status_not_held():
    message, should_delete = describe_lock_status(LockStatus(held=False), delete_if_expired=True)
    assert "No lock currently held" in message
    assert should_delete is False


def test_describe_lock_status_held_and_fresh_never_deletes():
    status = LockStatus(held=True, holder="dag:run-1", age=datetime.timedelta(minutes=5), expired=False)

    for delete_if_expired in (False, True):
        message, should_delete = describe_lock_status(status, delete_if_expired=delete_if_expired)
        assert "Not expired -- not deleting" in message
        assert should_delete is False


def test_describe_lock_status_held_and_expired_without_flag_does_not_delete():
    status = LockStatus(held=True, holder="dag:run-2", age=datetime.timedelta(hours=7), expired=True)

    message, should_delete = describe_lock_status(status, delete_if_expired=False)
    assert "EXPIRED" in message
    assert '-delete-if-expired' in message
    assert should_delete is False


def test_describe_lock_status_held_and_expired_with_flag_deletes():
    status = LockStatus(held=True, holder="dag:run-2", age=datetime.timedelta(hours=7), expired=True)

    message, should_delete = describe_lock_status(status, delete_if_expired=True)
    assert "EXPIRED" in message
    assert should_delete is True
