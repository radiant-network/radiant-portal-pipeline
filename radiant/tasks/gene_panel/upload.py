"""Send a tenant's gene panel file to the portal: one `PUT /{tenant}/gene_panels`.

The file goes up unchanged, as the multipart attachment. The portal owns the format: it parses
and checks the file, resolves the symbols, and replaces the tenant's uploaded gene panels in one
transaction. So this module does not read the file, it only moves it and reports the answer.

Same `radiant_api_conn` connection and client_credentials token as the other portal calls.
Airflow is imported inside the functions, like `radiant/tasks/nextflow/register.py`.
"""

import json
import logging
import posixpath

from radiant.tasks.nextflow.portal import PortalError, put_gene_panels
from radiant.tasks.nextflow.register import PORTAL_CONN_ID, load_portal_connection

LOGGER = logging.getLogger(__name__)

# A 4xx is the portal's answer to this file or this account: a retry sends the same file and
# gets the same answer. 408 and 429 are the exceptions, they say nothing about the request.
RETRYABLE_CLIENT_STATUSES = (408, 429)


# The portal's limit (413 above it). Checked before the download, so a wrong path to a large
# object fails at once instead of loading it into the worker's memory.
MAX_FILE_BYTES = 10 * 1024 * 1024

# S3 answers that a retry cannot change: the object is missing or the worker may not read it.
# HEAD returns the bare HTTP status as the code, GET the named error.
PERMANENT_S3_ERRORS = {"403", "404", "AccessDenied", "NoSuchKey", "NoSuchBucket"}


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/key` -> `(bucket, key)`. Fails on anything that does not name one object.

    Split by hand, not with urlparse: an S3 key may hold `?` or `#`, which urlparse would cut off.
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"'{uri}' is not the S3 URI of a file (expected s3://<bucket>/<key>)")
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not key or key.endswith("/"):
        raise ValueError(f"'{uri}' is not the S3 URI of a file (expected s3://<bucket>/<key>)")
    return bucket, key


def read_s3_file(uri: str) -> bytes:
    """Read the object, failing the task without a retry when it is missing, unreadable or too large."""
    import boto3
    from airflow.exceptions import AirflowFailException
    from botocore.exceptions import ClientError

    bucket, key = parse_s3_uri(uri)
    s3 = boto3.client("s3")
    try:
        size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
        if size > MAX_FILE_BYTES:
            raise AirflowFailException(
                f"{uri} is {size} bytes, more than the {MAX_FILE_BYTES} the portal accepts: check the path."
            )
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in PERMANENT_S3_ERRORS:
            raise AirflowFailException(f"cannot read {uri}: {code} (missing object or no read access)") from None
        raise


def _error_detail(body: str):
    """The `detail` of the portal's ApiError body, or the raw body when it is not that JSON."""
    try:
        return json.loads(body).get("detail")
    except (ValueError, AttributeError):
        return body


def _log_warnings(warnings: list[dict]) -> None:
    for warning in warnings:
        LOGGER.warning(
            "line %s, panel %s, symbol %s: %s",
            warning.get("line"),
            warning.get("panel_code"),
            warning.get("symbol"),
            warning.get("message"),
        )


def _failure_message(status: int, tenant: str, filepath: str, detail) -> str:
    prefix = f"PUT /{tenant}/gene_panels of {filepath} returned {status}"
    if status == 400:
        return f"{prefix}: the file is not valid, fix it and run again. Detail: {detail}"
    if status == 403:
        return (
            f"{prefix}. A valid token is not enough: the service-account user needs tenant access and "
            f"the `can_manage_analysis_catalog` action granted inside the portal for tenant '{tenant}'."
        )
    if status == 404:
        return (
            f"{prefix}: the portal has no gene panel upload endpoint (not deployed yet) or no tenant "
            f"'{tenant}'. Detail: {detail}"
        )
    if status == 409:
        return f"{prefix}: a panel_code of the file is already used by another panel of the tenant. Detail: {detail}"
    if status == 413:
        return f"{prefix}: the file is larger than the portal accepts (10 MiB)."
    if status == 422:
        return f"{prefix}: strict=true and some rows match no Ensembl gene (logged above). Nothing was changed."
    return f"{prefix}. Detail: {detail}"


def upload_gene_panels(tenant: str, filepath: str, strict: bool, conn_id: str = PORTAL_CONN_ID) -> dict:
    """Read the file from S3, attach it to one PUT, log the warnings, return the counts.

    A 4xx fails the task without a retry (the same file gets the same answer). A 5xx or a network
    error is raised as is, so Airflow retries it: the upload replaces the full set, so a retry is
    safe, for example when the portal committed but the MV refresh failed.
    """
    from airflow.exceptions import AirflowFailException

    content = read_s3_file(filepath)
    filename = posixpath.basename(parse_s3_uri(filepath)[1])
    LOGGER.info("uploading %s (%d bytes) to tenant '%s', strict=%s", filepath, len(content), tenant, strict)

    portal = load_portal_connection(conn_id)
    try:
        result = put_gene_panels(portal.host, tenant, portal.token(), filename, content, strict)
    except PortalError as e:
        status = e.status or 0
        if 400 <= status < 500 and status not in RETRYABLE_CLIENT_STATUSES:
            detail = _error_detail(e.body)
            if status == 422 and isinstance(detail, dict):
                _log_warnings(detail.get("warnings") or [])
            raise AirflowFailException(_failure_message(status, tenant, filepath, detail)) from None
        raise

    warnings = result.get("warnings") or []
    _log_warnings(warnings)
    summary = {"panels": result.get("panels"), "genes": result.get("genes"), "warnings": len(warnings)}
    LOGGER.info(
        "tenant '%s' now has %s uploaded gene panel(s), %s gene(s); %d row(s) skipped",
        tenant,
        summary["panels"],
        summary["genes"],
        len(warnings),
    )
    return summary
