from unittest.mock import MagicMock

import pytest

from radiant.tasks.nextflow import portal


def _response(status: int, payload: dict | None = None, text: str = "") -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.ok = status < 400
    response.text = text
    response.content = b"x" if payload is not None else b""
    response.json.return_value = payload
    return response


@pytest.fixture
def requests_mock(monkeypatch):
    mocked = MagicMock()
    monkeypatch.setattr("requests.post", mocked.post)
    monkeypatch.setattr("requests.patch", mocked.patch)
    monkeypatch.setattr("requests.get", mocked.get)
    monkeypatch.setattr("requests.put", mocked.put)
    return mocked


def test_post_case_group_sends_name_and_ids_and_returns_the_group(requests_mock):
    requests_mock.post.return_value = _response(200, {"name": "run-a", "tenant_code": "qlin", "case_ids": [1, 2]})

    group = portal.post_case_group("https://api/", "qlin", "tok", "run-a", [2, 1])

    assert group == {"name": "run-a", "tenant_code": "qlin", "case_ids": [1, 2]}
    _, kwargs = requests_mock.post.call_args
    assert requests_mock.post.call_args.args[0] == "https://api/qlin/case_groups"
    assert kwargs["json"] == {"name": "run-a", "case_ids": [2, 1]}
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}


def test_post_case_group_400_lists_the_unknown_ids(requests_mock):
    requests_mock.post.return_value = _response(400, text='{"message":"unknown case ids in this tenant: [99]"}')

    with pytest.raises(portal.PortalError, match="unknown case ids"):
        portal.post_case_group("https://api", "qlin", "tok", "run-a", [99])


def test_post_case_group_403_names_the_missing_grant(requests_mock):
    requests_mock.post.return_value = _response(403)

    with pytest.raises(portal.PortalError, match="ingest_data.*tenant 'qlin'"):
        portal.post_case_group("https://api", "qlin", "tok", "run-a", [1])


def test_notify_case_group_posts_without_body_and_returns_the_report(requests_mock):
    report = {"group": {"name": "run-a"}, "emails": [{"organization_code": "LDM-A", "status": "sent"}]}
    requests_mock.post.return_value = _response(200, report)

    assert portal.notify_case_group("https://api", "qlin", "tok", "run-a") == report
    assert requests_mock.post.call_args.args[0] == "https://api/qlin/case_groups/run-a/notify"
    assert "json" not in requests_mock.post.call_args.kwargs


def test_notify_case_group_404_and_500_are_errors(requests_mock):
    requests_mock.post.return_value = _response(404, text='{"message":"case group not found"}')
    with pytest.raises(portal.PortalError, match="404"):
        portal.notify_case_group("https://api", "qlin", "tok", "nope")

    requests_mock.post.return_value = _response(500, text='{"message":"Internal Server Error"}')
    with pytest.raises(portal.PortalError, match="500"):
        portal.notify_case_group("https://api", "qlin", "tok", "run-a")


# Not valid UTF-8 on purpose, with a CRLF and a trailing tab: any decode or re-encode would change it.
GENE_PANEL_FILE = b"panel_code\tpanel_name\tsymbol\r\nPANEL_1\tCardio\tTTN\t\n\xff\xfe"


def test_put_gene_panels_attaches_the_file_unchanged(requests_mock):
    import requests

    requests_mock.put.return_value = _response(200, {"panels": 1, "genes": 1, "warnings": []})

    result = portal.put_gene_panels("https://api/", "radiant", "tok", "gene_panels.tsv", GENE_PANEL_FILE, False)

    assert result == {"panels": 1, "genes": 1, "warnings": []}
    assert requests_mock.put.call_count == 1
    args, kwargs = requests_mock.put.call_args
    assert args[0] == "https://api/radiant/gene_panels"
    assert kwargs["params"] == {"strict": "false"}
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}
    assert kwargs["files"] == {"file": ("gene_panels.tsv", GENE_PANEL_FILE, "text/tab-separated-values")}
    # The bytes reach the wire as they are, in the one part named file.
    body = requests.Request("PUT", args[0], files=kwargs["files"]).prepare().body
    assert b'name="file"; filename="gene_panels.tsv"' in body
    assert GENE_PANEL_FILE in body


def test_put_gene_panels_sends_strict(requests_mock):
    requests_mock.put.return_value = _response(200, {"panels": 0, "genes": 0, "warnings": []})

    portal.put_gene_panels("https://api", "radiant", "tok", "f.tsv", b"x", True)

    assert requests_mock.put.call_args.kwargs["params"] == {"strict": "true"}


def test_put_gene_panels_error_keeps_status_and_body(requests_mock):
    body = '{"status":400,"message":"bad file","detail":{"line":4}}'
    requests_mock.put.return_value = _response(400, text=body)

    with pytest.raises(portal.PortalError, match="bad file") as e:
        portal.put_gene_panels("https://api", "radiant", "tok", "f.tsv", b"x", False)

    assert e.value.status == 400
    assert e.value.body == body
