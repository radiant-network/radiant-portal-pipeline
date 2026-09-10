"""Send one tenant's batch to the portal and insist on a confirmed result.

Extracted from `nextflow_postprocessing_cases.register_tasks` so the QC DAG registers the
same way. Airflow is imported inside the function: the module stays importable from plain
unit tests, and only the DAG task pays for it.
"""

import json
import logging

from radiant.tasks.nextflow.portal import fetch_token, patch_case_batch, wait_for_batch

LOGGER = logging.getLogger(__name__)

PORTAL_CONN_ID = "radiant_api_conn"

SUCCESS_STATUSES = ("success", "succeeded", "completed", "done")


def register_case_batch(tenant: str, body: dict, dry_run: bool, conn_id: str = PORTAL_CONN_ID) -> dict:
    """PATCH `body` into `tenant`, wait for the batch, and fail loudly on anything but success.

    Returning green without a confirmed batch would be the worst outcome available: nothing
    registered, so the cases stay eligible and the pipeline redoes them tomorrow, and the
    night after.
    """
    from airflow.exceptions import AirflowFailException, AirflowNotFoundException
    from airflow.hooks.base import BaseHook

    try:
        conn = BaseHook.get_connection(conn_id)
    except AirflowNotFoundException:
        raise AirflowFailException(
            f"Connection '{conn_id}' is not configured. It needs host = the API url, "
            f"login = the OIDC client id, password = its secret, and extra = "
            f'{{"token_url": "...", "scope": "..."}}.'
        ) from None

    extra = conn.extra_dejson
    if not extra.get("token_url"):
        raise AirflowFailException(f"Connection '{conn_id}' extra is missing 'token_url'.")

    n_tasks = sum(len(c["tasks"]) for c in body["cases"])
    n_docs = sum(len(t["output_documents"]) for c in body["cases"] for t in c["tasks"])
    LOGGER.info(
        "%s into tenant '%s': %d case(s), %d task(s), %d output document(s)",
        "dry run" if dry_run else "registering",
        tenant,
        len(body["cases"]),
        n_tasks,
        n_docs,
    )

    token = fetch_token(extra["token_url"], conn.login, conn.password, extra.get("scope"))
    batch_id = patch_case_batch(conn.host, tenant, token, body, dry_run)
    LOGGER.info("batch id: %s", batch_id)
    if not batch_id:
        raise AirflowFailException(
            f"the portal accepted the batch for tenant '{tenant}' but reported no batch id, "
            f"so nothing can be confirmed as registered"
        )

    report = wait_for_batch(conn.host, tenant, token, batch_id)
    LOGGER.info("batch report:\n%s", json.dumps(report, indent=2, default=str)[:8000])
    if str(report.get("status", "")).lower() not in SUCCESS_STATUSES:
        raise AirflowFailException(f"batch {batch_id} did not succeed: status={report.get('status')}")
    return report
