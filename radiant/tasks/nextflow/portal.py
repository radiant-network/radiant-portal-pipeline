"""Minimal Radiant portal API client: a token, the case batch PATCH and its poll, the case
system status PATCH, the case group calls, and the gene panel upload.

Deliberately not the generated `radiant_python` client -- it is not published to an index,
and each call here is one plain request whose body we build ourselves.

Authentication is the OAuth **client_credentials** grant: a client id and secret, no
browser and no device approval. Note that a valid token is not sufficient on its own. The
portal authorises against its own permission store -- tenant access plus an action, not realm
roles -- so a service-account client that has not been granted those gets a flat 403 before
any validation runs, with no error codes to read. The action is `ingest_data` for the case
calls and `can_manage_analysis_catalog` for the gene panel upload.
"""

import logging
import time

import requests

LOGGER = logging.getLogger(__name__)

TOKEN_TIMEOUT_SECONDS = 30
REQUEST_TIMEOUT_SECONDS = 300
BATCH_POLL_INTERVAL_SECONDS = 5
BATCH_POLL_TIMEOUT_SECONDS = 600

PENDING_STATUSES = {"pending", "processing", "in_progress", "running"}

# The only case status changes the pipeline makes: target status -> the status the case must
# still be in. Anything else is a user status, which the pipeline never touches.
SYSTEM_STATUS_TRANSITIONS = {"processing": "submitted", "in_progress": "processing"}


class PortalError(Exception):
    """The portal refused a request, or reported a failed batch.

    `status` and `body` are set when the portal answered, so a caller can tell a refusal of the
    request (4xx) from a failure worth a retry.
    """

    def __init__(self, message: str, status: int | None = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


def fetch_token(token_url: str, client_id: str, client_secret: str, scope: str | None = None) -> str:
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


def _raise_for_status(response, method: str, url: str, tenant: str) -> None:
    if response.status_code == 403:
        raise PortalError(
            f"{method} {url} returned 403. A valid token is not enough: the service-account user "
            f"needs tenant access and the `ingest_data` action granted inside the portal for "
            f"tenant '{tenant}'."
        )
    if not response.ok:
        raise PortalError(f"{method} {url} failed: {response.status_code} {response.text[:2000]}")


def patch_case_system_status(host: str, tenant: str, token: str, cases: list[dict]) -> list[dict]:
    """Send the status changes in one `PATCH /{tenant}/cases/status`; returns the portal's
    `[{case_id, updated, current_status_code}]`.

    The portal checks every change before writing any, so a 400 or a 403 changes no case. It then
    writes them one by one: a 404 (a case deleted between the check and its write) leaves the
    cases before it changed, which a retry reports as `updated: false` already in their target.
    """

    for case in cases:
        status_code = case.get("status_code")
        expected = SYSTEM_STATUS_TRANSITIONS.get(status_code)
        if expected is None or case.get("expected_status_codes") != [expected]:
            raise ValueError(
                f"case {case.get('case_id')}: the pipeline only sends {SYSTEM_STATUS_TRANSITIONS} "
                f"(target: expected), got {status_code}: {case.get('expected_status_codes')}"
            )

    url = f"{host.rstrip('/')}/{tenant}/cases/status"
    response = requests.patch(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"cases": cases},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code == 403:
        raise PortalError(
            f"PATCH {url} returned 403 and changed no case. Either the service-account user is "
            f"missing the `can_ingest_data` action at every diagnosis lab ('*') of tenant '{tenant}', or "
            f"one of the cases is not in that tenant.",
            status=403,
            body=response.text,
        )
    if not response.ok:
        raise PortalError(
            f"PATCH {url} failed: {response.status_code} {response.text[:2000]}",
            status=response.status_code,
            body=response.text,
        )
    return response.json()["cases"]


def set_case_system_status(host: str, tenant: str, token: str, case_ids: list[int], status_code: str) -> list[dict]:
    """Move the cases to `processing` (from `submitted`) or `in_progress` (from `processing`)."""
    if status_code not in SYSTEM_STATUS_TRANSITIONS:
        raise ValueError(
            f"'{status_code}' is not a pipeline status, expected one of {list(SYSTEM_STATUS_TRANSITIONS)}"
        )
    if not case_ids:
        return []

    expected = SYSTEM_STATUS_TRANSITIONS[status_code]
    cases = [
        {"case_id": case_id, "status_code": status_code, "expected_status_codes": [expected]}
        for case_id in sorted(set(case_ids))
    ]
    results = patch_case_system_status(host, tenant, token, cases)

    skipped = [r for r in results if not r.get("updated")]
    for result in skipped:
        if result.get("current_status_code") == status_code:
            LOGGER.info("tenant '%s': case %s is already %s", tenant, result.get("case_id"), status_code)
            continue
        LOGGER.warning(
            "tenant '%s': case %s not moved to %s, its status is %s (expected %s)",
            tenant,
            result.get("case_id"),
            status_code,
            result.get("current_status_code"),
            expected,
        )
    LOGGER.info(
        "tenant '%s': %d case(s) moved to %s, %d left unchanged",
        tenant,
        len(results) - len(skipped),
        status_code,
        len(skipped),
    )
    return results


def post_case_group(host: str, tenant: str, token: str, name: str, case_ids: list[int]) -> dict:
    """Create the named case group, or overwrite its case list when the name already exists.

    The name is the key, so a retried task lands on the same group instead of a second one.
    Returns the group as the portal stores it: `{name, tenant_code, case_ids}`.
    """

    url = f"{host.rstrip('/')}/{tenant}/case_groups"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"name": name, "case_ids": case_ids},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    _raise_for_status(response, "POST", url, tenant)
    return response.json()


def put_gene_panels(host: str, tenant: str, token: str, filename: str, content: bytes, strict: bool) -> dict:
    """Replace the tenant's uploaded gene panels with the panels of the file; returns
    `{panels, genes, warnings}`.

    The file is the one multipart part, named `file`, sent as is. The portal needs the
    `can_manage_analysis_catalog` action here, not `ingest_data`, so a 403 is not reported with
    the generic hint. Any refusal raises PortalError with the status and the body (the ApiError
    `detail` holds the line of a 400 and the rows of a 422).
    """

    url = f"{host.rstrip('/')}/{tenant}/gene_panels"
    response = requests.put(
        url,
        params={"strict": str(strict).lower()},
        headers={"Authorization": f"Bearer {token}"},
        files={"file": (filename, content, "text/tab-separated-values")},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if not response.ok:
        raise PortalError(
            f"PUT {url} failed: {response.status_code} {response.text[:2000]}",
            status=response.status_code,
            body=response.text,
        )
    return response.json()


def notify_case_group(host: str, tenant: str, token: str, name: str) -> dict:
    """Email each diagnosis laboratory of the group its manifest; returns the per-lab report.

    Stateless on the portal side: calling it again sends again. A 500 means nothing was sent
    (no template for the tenant, or bad SMTP settings), a 404 that the group does not exist.
    """

    url = f"{host.rstrip('/')}/{tenant}/case_groups/{name}/notify"
    response = requests.post(url, headers={"Authorization": f"Bearer {token}"}, timeout=REQUEST_TIMEOUT_SECONDS)
    _raise_for_status(response, "POST", url, tenant)
    return response.json()
