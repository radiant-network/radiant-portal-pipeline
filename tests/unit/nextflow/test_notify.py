from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowFailException

from radiant.tasks.nextflow import notify
from radiant.tasks.nextflow.register import PortalConnection

CONN = PortalConnection(host="https://api", token_url="https://kc/token", client_id="airflow", client_secret="s")


@pytest.fixture
def portal(monkeypatch):
    monkeypatch.setattr(notify, "load_portal_connection", lambda conn_id: CONN)
    monkeypatch.setattr(PortalConnection, "token", lambda self: "tok")
    mocked = MagicMock()
    monkeypatch.setattr(notify, "post_case_group", mocked.post_case_group)
    monkeypatch.setattr(notify, "notify_case_group", mocked.notify_case_group)
    return mocked


def _report(*statuses: str) -> dict:
    return {
        "group": {"name": "postprocessing-run", "tenant_code": "qlin", "case_ids": [1, 2]},
        "emails": [
            {
                "organization_code": f"LDM-{i}",
                "status": status,
                "error": "relay refused" if status == "failed" else "",
                "recipients": ["a@lab.invalid"] if status != "skipped_no_contact" else [],
                "case_count": 1,
                "document_count": 3,
                "template": "manifest_qlin.tmpl",
                "context": {
                    "has_stat": False,
                    "analysis_codes": ["WGS"],
                    "case_ids": [i],
                    "manifest_filename": "m.tsv",
                },
            }
            for i, status in enumerate(statuses)
        ],
    }


def test_group_name_is_prefix_plus_run_tag():
    assert notify.group_name("postprocessing", "scheduled__2026-09-03T00:00:00+00:00") == (
        "postprocessing-scheduled-2026-09-03T00-00-00-00-00"
    )


def test_post_group_posts_the_tenant_ids(portal):
    portal.post_case_group.return_value = {"name": "g", "tenant_code": "qlin", "case_ids": [1, 2]}

    group = notify.post_group("qlin", "g", [1, 2])

    portal.post_case_group.assert_called_once_with("https://api", "qlin", "tok", "g", [1, 2])
    assert group["case_ids"] == [1, 2]


def test_send_notification_returns_the_report_when_every_lab_is_sent_or_skipped(portal, caplog):
    portal.notify_case_group.return_value = _report("sent", "skipped_no_contact", "skipped_no_documents")

    with caplog.at_level("INFO"):
        report = notify.send_notification("qlin", "g")

    portal.notify_case_group.assert_called_once_with("https://api", "qlin", "tok", "g")
    assert report["group"]["name"] == "postprocessing-run"
    assert "lab LDM-0: sent" in caplog.text
    assert "template=manifest_qlin.tmpl" in caplog.text
    assert [r.levelname for r in caplog.records if "lab LDM-1" in r.message] == ["WARNING"]


def test_send_notification_fails_when_one_lab_failed_but_names_it(portal):
    portal.notify_case_group.return_value = _report("sent", "failed")

    with pytest.raises(AirflowFailException, match=r"failed for lab\(s\) \['LDM-1'\]"):
        notify.send_notification("qlin", "g")


def test_send_notification_500_propagates_as_portal_error(portal):
    from radiant.tasks.nextflow.portal import PortalError

    portal.notify_case_group.side_effect = PortalError("POST ... failed: 500")

    with pytest.raises(PortalError, match="500"):
        notify.send_notification("qlin", "g")
