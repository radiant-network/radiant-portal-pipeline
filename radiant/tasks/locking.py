import dataclasses
import datetime
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("airflow.task")

LOCK_KEY_PREFIX = "_locks"
STALE_LOCK_MAX_AGE = datetime.timedelta(hours=6)

IMPORT_MUTEX_LOCK_NAME = "import_mutex"


class LockHeldError(Exception):
    """Raised when a mutex is already held by another run."""


@dataclasses.dataclass(frozen=True)
class LockStatus:
    held: bool
    holder: str | None = None
    age: datetime.timedelta | None = None
    expired: bool = False


def _lock_key(name: str) -> str:
    return f"{LOCK_KEY_PREFIX}/{name}"


def acquire_lock(bucket: str, name: str, holder: str) -> None:
    s3 = boto3.client("s3")
    key = _lock_key(name)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=holder.encode(), IfNoneMatch="*")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("412", "PreconditionFailed"):
            raise LockHeldError(f"Lock {key} is already held by another run.") from e
        raise
    logger.info(f"Acquired lock {key} for {holder}.")


def release_lock(bucket: str, name: str) -> None:
    s3 = boto3.client("s3")
    key = _lock_key(name)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Released lock {key}.")


def check_lock(bucket: str, name: str, stale_after: datetime.timedelta = STALE_LOCK_MAX_AGE) -> LockStatus:
    s3 = boto3.client("s3")
    key = _lock_key(name)

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return LockStatus(held=False)
        raise

    holder = obj["Body"].read().decode()
    age = datetime.datetime.now(datetime.UTC) - obj["LastModified"]
    return LockStatus(held=True, holder=holder, age=age, expired=age > stale_after)


def describe_lock_status(status: LockStatus, delete_if_expired: bool) -> tuple[str, bool]:
    if not status.held:
        return "No lock currently held.", False

    if not status.expired:
        ttl_remaining = STALE_LOCK_MAX_AGE - status.age
        return (
            f"Lock held by {status.holder!r}, age={status.age}, TTL remaining={ttl_remaining}. "
            "Not expired -- not deleting.",
            False,
        )

    if delete_if_expired:
        return f"Lock held by {status.holder!r}, age={status.age} -- EXPIRED. Deleting.", True

    return (
        f"Lock held by {status.holder!r}, age={status.age} -- EXPIRED (max age {STALE_LOCK_MAX_AGE}). "
        "delete_if_expired=False -- not deleting. Re-run with delete_if_expired=True to clear it.",
        False,
    )
