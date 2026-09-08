"""S3-backed mutual-exclusion lock.

See design/SJRA-1811-opendatalake-integration.md §4 (Decision 2): an Airflow pool caps
concurrent *task* count, it can't express "this whole run must finish before that run starts".
This lock is a single object under `_locks/` in the Airflow DAGs bucket, acquired with a
conditional `PutObject` (`IfNoneMatch="*"`) that only one caller can win, and released with a
plain `DeleteObject`.

`acquire_lock` never deletes anything -- a run that fails leaves its lock in place, on purpose:
`import_part` failures are routine and get restarted by an operator, and an automatic reclaim
would let that restart race whatever else is running. Clearing an abandoned lock is a deliberate,
separate action (the toolbox DAG's `check-lock` command), gated on `check_lock` reporting it expired
*and* an operator explicitly asking for the delete.

Note that MinIO does not honor the `*` wildcard for `If-None-Match`
(https://github.com/minio/minio/issues/20346, open), so the conditional-write behavior this
module depends on can only be exercised against real S3, not the `USE_DOCKER_FIXTURES=true`
MinIO fixture.
"""

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
    """Point-in-time read of a lock, for reporting -- never acted on automatically."""

    held: bool
    holder: str | None = None
    age: datetime.timedelta | None = None
    expired: bool = False


def _lock_key(name: str) -> str:
    return f"{LOCK_KEY_PREFIX}/{name}"


def acquire_lock(bucket: str, name: str, holder: str) -> None:
    """Acquire a named mutex via a conditional S3 write.

    Raises:
        LockHeldError: another run already holds the lock (live or abandoned -- this
            function never deletes an existing lock to find out which).
    """
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
    """Delete a named mutex. A no-op if the lock object is already gone.

    The only two callers that should ever exist: the DAG that holds the lock, releasing it
    on its own successful completion, and an operator clearing a lock `check_lock` reported
    as expired.
    """
    s3 = boto3.client("s3")
    key = _lock_key(name)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Released lock {key}.")


def check_lock(bucket: str, name: str, stale_after: datetime.timedelta = STALE_LOCK_MAX_AGE) -> LockStatus:
    """Report a lock's holder/age/expiry without ever modifying it."""
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
    """Report a lock's status in one line and decide whether it should be deleted.

    Returns:
        (message, should_delete) -- `should_delete` is only ever True when the lock is both
        expired and `delete_if_expired` was explicitly set; a live lock is never deleted,
        regardless of the flag.
    """
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
