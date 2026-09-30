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
