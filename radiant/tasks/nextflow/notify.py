"""Group a run's cases in the portal and have it email the diagnosis laboratories.

Two portal calls, both `ingest_data` in-tenant like the batch PATCH: `POST /case_groups`
(create or overwrite by name, so a retry is idempotent) and `POST /case_groups/{name}/notify`
(one email per lab with its TSV manifest, stateless, so a retry re-sends). Airflow is imported
inside the functions for the same reason as `register.py`.
"""

import json
import logging

from radiant.tasks.nextflow.paths import sanitize_run_tag
from radiant.tasks.nextflow.portal import notify_case_group, post_case_group
from radiant.tasks.nextflow.register import PORTAL_CONN_ID, load_portal_connection

LOGGER = logging.getLogger(__name__)

FAILED = "failed"


def group_name(prefix: str, run_id: str) -> str:
    """`<prefix>-<run tag>`: dated, unique per run, stable across retries, and a valid portal name."""
    return f"{prefix}-{sanitize_run_tag(run_id)}"


def post_group(tenant: str, name: str, case_ids: list[int], conn_id: str = PORTAL_CONN_ID) -> dict:
    portal = load_portal_connection(conn_id)
    LOGGER.info("case group '%s' in tenant '%s': %d case(s)", name, tenant, len(case_ids))
    group = post_case_group(portal.host, tenant, portal.token(), name, case_ids)
    LOGGER.info("case group stored: %s", json.dumps(group, default=str))
    return group


def send_notification(tenant: str, name: str, conn_id: str = PORTAL_CONN_ID) -> dict:
    """Trigger the notification and log its report; fail only when a lab's email failed.

    `skipped_no_contact` and `skipped_no_documents` are logged as warnings, not failures: the
    first is a lab without a distribution list (an admin task, not a pipeline one), the second
    a lab whose cases produced nothing to send.
    """
    from airflow.exceptions import AirflowFailException

    portal = load_portal_connection(conn_id)
    report = notify_case_group(portal.host, tenant, portal.token(), name)
    failed = []
    for email in report.get("emails", []):
        status = email.get("status")
        line = (
            f"lab {email.get('organization_code')}: {status}, {email.get('case_count')} case(s), "
            f"{email.get('document_count')} document(s), recipients={email.get('recipients')}, "
            f"template={email.get('template')}, context={json.dumps(email.get('context'), default=str)}"
        )
        if status == FAILED:
            failed.append(email.get("organization_code"))
            LOGGER.error("%s, error=%s", line, email.get("error"))
        elif status == "sent":
            LOGGER.info(line)
        else:
            LOGGER.warning(line)
    if failed:
        raise AirflowFailException(
            f"notification of case group '{name}' failed for lab(s) {failed}, see the report above"
        )
    return report
