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
    mock_client.get_object.return_value = {"Body": io.BytesIO(b"dag:run-9")}

    with (
        patch("radiant.tasks.locking.boto3.client", return_value=mock_client),
        pytest.raises(LockHeldError) as excinfo,
    ):
        acquire_lock(bucket="warehouse", name="import_mutex", holder="dag:run-1")

    # The holder is the one thing an operator needs from this error: it says which run to go look at,
    # and it distinguishes a real conflict from this run tripping over its own earlier attempt.
    assert "dag:run-9" in str(excinfo.value)
    mock_client.delete_object.assert_not_called()


def test_acquire_lock_still_raises_when_the_holder_cannot_be_read():
    # Reading the holder only decorates the message. If that read fails, the caller must still see
    # LockHeldError -- not whatever the GET raised.
    mock_client = MagicMock()
    mock_client.put_object.side_effect = _client_error("412")
    mock_client.get_object.side_effect = _client_error("404")

    with (
        patch("radiant.tasks.locking.boto3.client", return_value=mock_client),
        pytest.raises(LockHeldError) as excinfo,
    ):
        acquire_lock(bucket="warehouse", name="import_mutex", holder="dag:run-1")

    assert "another run" in str(excinfo.value)


def test_acquire_lock_raises_even_when_existing_lock_is_stale():
    # acquire_lock never deletes anything, however old the existing lock is -- clearing an
    # abandoned lock is a separate, deliberate action (the toolbox DAG's check-lock command).
    mock_client = MagicMock()
    mock_client.put_object.side_effect = _client_error("412")
    mock_client.get_object.return_value = {"Body": io.BytesIO(b"dag:run-1")}

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


def test_release_lock_without_a_holder_deletes_whatever_is_there():
    # The operator override, used by the toolbox DAG's check-lock command. It does not read first.
    mock_client = MagicMock()

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        release_lock(bucket="warehouse", name="import_mutex")

    mock_client.delete_object.assert_called_once_with(Bucket="warehouse", Key="_locks/import_mutex")
    mock_client.get_object.assert_not_called()


def test_release_lock_deletes_the_key_when_this_run_holds_it():
    mock_client = MagicMock()
    mock_client.get_object.return_value = {"Body": io.BytesIO(b"dag:run-1")}

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        release_lock(bucket="warehouse", name="import_mutex", holder="dag:run-1")

    mock_client.delete_object.assert_called_once_with(Bucket="warehouse", Key="_locks/import_mutex")


def test_release_lock_refuses_to_free_a_lock_another_run_holds():
    """The reason this check exists.

    `import_part` releases on ALL_DONE, so the release task runs even when `acquire_import_lock` failed
    because the re-annotation DAG holds the lock. An unconditional delete there would hand that DAG's
    mutex to the next import while it is still running.
    """
    mock_client = MagicMock()
    mock_client.get_object.return_value = {"Body": io.BytesIO(b"radiant-reannotate-open-data:run-7")}

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        release_lock(bucket="warehouse", name="import_mutex", holder="radiant-import-part:run-1")

    mock_client.delete_object.assert_not_called()


def test_release_lock_does_nothing_when_the_lock_is_already_gone():
    # Nothing to free, and nothing to complain about -- a retried release must stay idempotent.
    mock_client = MagicMock()
    mock_client.get_object.side_effect = _client_error("404")

    with patch("radiant.tasks.locking.boto3.client", return_value=mock_client):
        release_lock(bucket="warehouse", name="import_mutex", holder="dag:run-1")

    mock_client.delete_object.assert_not_called()


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
    assert "-delete-if-expired" in message
    assert should_delete is False


def test_describe_lock_status_held_and_expired_with_flag_deletes():
    status = LockStatus(held=True, holder="dag:run-2", age=datetime.timedelta(hours=7), expired=True)

    message, should_delete = describe_lock_status(status, delete_if_expired=True)
    assert "EXPIRED" in message
    assert should_delete is True


def test_describe_lock_status_force_deletes_a_fresh_lock():
    """-force-delete is the only flag that clears a lock still inside its TTL.

    That is the whole point of it: a holder that died without releasing leaves a lock that
    -delete-if-expired will not touch for up to 6h.
    """
    status = LockStatus(held=True, holder="dag:run-1", age=datetime.timedelta(minutes=5), expired=False)

    message, should_delete = describe_lock_status(status, delete_if_expired=False, force_delete=True)
    assert should_delete is True
    # The message has to say the quiet part: this may be taking the lock off a run that is still going.
    assert "FORCE DELETING" in message
    assert "dag:run-1" in message


def test_describe_lock_status_force_deletes_an_expired_lock_too():
    status = LockStatus(held=True, holder="dag:run-2", age=datetime.timedelta(hours=7), expired=True)

    _, should_delete = describe_lock_status(status, delete_if_expired=False, force_delete=True)
    assert should_delete is True


def test_describe_lock_status_force_delete_has_nothing_to_do_when_no_lock_is_held():
    message, should_delete = describe_lock_status(LockStatus(held=False), delete_if_expired=False, force_delete=True)
    assert should_delete is False
    assert "No lock currently held" in message


def test_describe_lock_status_defaults_to_not_forcing():
    # The existing two-argument callers must keep their old behaviour: a fresh lock stays put.
    status = LockStatus(held=True, holder="dag:run-1", age=datetime.timedelta(minutes=5), expired=False)

    _, should_delete = describe_lock_status(status, delete_if_expired=True)
    assert should_delete is False
