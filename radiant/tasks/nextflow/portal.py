"""Minimal Radiant portal API client: a token, a batch PATCH, and a poll.

Deliberately not the generated `radiant_python` client -- it is not published to an index,
and the two calls used here are a PATCH with a body we build ourselves and a GET.

Authentication is the OAuth **client_credentials** grant: a client id and secret, no
browser and no device approval. Note that a valid token is not sufficient on its own. The
portal authorises against its own permission store -- tenant access plus the `ingest_data`
action -- not against realm roles, so a service-account client that has not been granted
those gets a flat 403 before any validation runs, with no error codes to read.
"""

import logging
import time

LOGGER = logging.getLogger(__name__)

TOKEN_TIMEOUT_SECONDS = 30
REQUEST_TIMEOUT_SECONDS = 300
BATCH_POLL_INTERVAL_SECONDS = 5
BATCH_POLL_TIMEOUT_SECONDS = 600

PENDING_STATUSES = {"pending", "processing", "in_progress", "running"}


class PortalError(Exception):
    """The portal refused a request, or reported a failed batch."""


def fetch_token(token_url: str, client_id: str, client_secret: str, scope: str | None = None) -> str:
    import requests

    data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    if scope:
        data["scope"] = scope
    response = requests.post(token_url, data=data, timeout=TOKEN_TIMEOUT_SECONDS)
    if not response.ok:
        # The body can echo the client id; the secret is never in it, but keep it short.
        raise PortalError(f"token request to {token_url} failed: {response.status_code} {response.text[:500]}")
    return response.json()["access_token"]


def patch_case_batch(host: str, tenant: str, token: str, body: dict, dry_run: bool) -> str | None:
    """Submit the batch. Returns its id, or None if the portal did not report one."""
    import requests

    url = f"{host.rstrip('/')}/{tenant}/cases/batch"
    response = requests.patch(
        url,
        params={"dry_run": str(dry_run).lower()},
        headers={"Authorization": f"Bearer {token}"},
        json=body,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code == 403:
        raise PortalError(
            f"PATCH {url} returned 403. A valid token is not enough: the service-account user "
            f"needs tenant access and the `ingest_data` action granted inside the portal for "
            f"tenant '{tenant}'."
        )
    if not response.ok:
        raise PortalError(f"PATCH {url} failed: {response.status_code} {response.text[:2000]}")

    payload = response.json() if response.content else {}
    return payload.get("batch_id") or payload.get("id")


def wait_for_batch(
    host: str,
    tenant: str,
    token: str,
    batch_id: str,
    poll_interval: int = BATCH_POLL_INTERVAL_SECONDS,
    timeout: int = BATCH_POLL_TIMEOUT_SECONDS,
) -> dict:
    """Poll until the batch leaves a pending state, then return its report.

    The report names every failure with its code and path, which is far better triage than
    an HTTP status -- so it is returned even when the batch failed, and logged by the
    caller before anything is raised.
    """
    import requests

    url = f"{host.rstrip('/')}/{tenant}/batches/{batch_id}"
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + timeout
    report: dict = {}
    while time.monotonic() < deadline:
        response = requests.get(url, headers=headers, timeout=TOKEN_TIMEOUT_SECONDS)
        if not response.ok:
            raise PortalError(f"GET {url} failed: {response.status_code} {response.text[:500]}")
        report = response.json()
        status = str(report.get("status", "")).lower()
        if status and status not in PENDING_STATUSES:
            return report
        time.sleep(poll_interval)
    raise PortalError(f"batch {batch_id} was still pending after {timeout}s; last report: {report}")
