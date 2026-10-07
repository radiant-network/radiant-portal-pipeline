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


def test_set_case_system_status_sends_the_expected_starting_status(requests_mock):
    requests_mock.patch.return_value = _response(
        200,
        {
            "cases": [
                {"case_id": 123, "updated": True, "current_status_code": "in_progress"},
                {"case_id": 456, "updated": True, "current_status_code": "in_progress"},
            ]
        },
    )

    results = portal.set_case_system_status("https://api/", "qlin", "tok", [456, 123, 456], "in_progress")

    assert [r["case_id"] for r in results] == [123, 456]
    args, kwargs = requests_mock.patch.call_args
    assert args[0] == "https://api/qlin/cases/status"
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}
    assert kwargs["json"] == {
        "cases": [
            {"case_id": 123, "status_code": "in_progress", "expected_status_codes": ["processing"]},
            {"case_id": 456, "status_code": "in_progress", "expected_status_codes": ["processing"]},
        ]
    }


def test_set_case_system_status_processing_expects_submitted(requests_mock):
    requests_mock.patch.return_value = _response(
        200, {"cases": [{"case_id": 1, "updated": True, "current_status_code": "processing"}]}
    )

    portal.set_case_system_status("https://api", "qlin", "tok", [1], "processing")

    assert requests_mock.patch.call_args.kwargs["json"] == {
        "cases": [{"case_id": 1, "status_code": "processing", "expected_status_codes": ["submitted"]}]
    }


def test_set_case_system_status_logs_updated_false_without_failing(requests_mock, caplog):
    requests_mock.patch.return_value = _response(
        200,
        {
            "cases": [
                {"case_id": 123, "updated": True, "current_status_code": "in_progress"},
                {"case_id": 456, "updated": False, "current_status_code": "revoked"},
            ]
        },
    )

    with caplog.at_level("WARNING", logger=portal.LOGGER.name):
        results = portal.set_case_system_status("https://api", "qlin", "tok", [123, 456], "in_progress")

    assert results[1] == {"case_id": 456, "updated": False, "current_status_code": "revoked"}
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "case 456" in warnings[0].getMessage()
    assert "revoked" in warnings[0].getMessage()


def test_set_case_system_status_403_names_the_missing_grant(requests_mock):
    requests_mock.patch.return_value = _response(403, text='{"message":"forbidden"}')

    with pytest.raises(portal.PortalError, match="`can_ingest_data`.*tenant 'qlin'") as e:
        portal.set_case_system_status("https://api", "qlin", "tok", [1], "processing")

    assert e.value.status == 403


def test_set_case_system_status_already_in_target_is_not_a_warning(requests_mock, caplog):
    requests_mock.patch.return_value = _response(
        200, {"cases": [{"case_id": 1, "updated": False, "current_status_code": "processing"}]}
    )

    with caplog.at_level("INFO", logger=portal.LOGGER.name):
        portal.set_case_system_status("https://api", "qlin", "tok", [1], "processing")

    assert not [r for r in caplog.records if r.levelname == "WARNING"]
    assert any("already processing" in r.getMessage() for r in caplog.records)


def test_set_case_system_status_404_keeps_the_status(requests_mock):
    requests_mock.patch.return_value = _response(404, text='{"message":"case 2 not found"}')

    with pytest.raises(portal.PortalError, match="404") as e:
        portal.set_case_system_status("https://api", "qlin", "tok", [1, 2], "processing")

    assert e.value.status == 404


def test_set_case_system_status_without_cases_sends_nothing(requests_mock):
    assert portal.set_case_system_status("https://api", "qlin", "tok", [], "processing") == []
    requests_mock.patch.assert_not_called()


@pytest.mark.parametrize("status_code", ["submitted", "draft", "completed", "revoked", "in_review", ""])
def test_set_case_system_status_refuses_any_other_status(requests_mock, status_code):
    with pytest.raises(ValueError, match="not a pipeline status"):
        portal.set_case_system_status("https://api", "qlin", "tok", [1], status_code)
    requests_mock.patch.assert_not_called()


@pytest.mark.parametrize(
    "case",
    [
        {"case_id": 1, "status_code": "completed", "expected_status_codes": ["in_progress"]},
        {"case_id": 1, "status_code": "in_progress", "expected_status_codes": ["submitted"]},
        {"case_id": 1, "status_code": "processing", "expected_status_codes": ["submitted", "in_review"]},
        {"case_id": 1, "status_code": "processing", "expected_status_codes": []},
        {"case_id": 1, "status_code": "processing"},
    ],
)
def test_patch_case_system_status_refuses_anything_but_the_two_system_changes(requests_mock, case):
    with pytest.raises(ValueError, match="only sends"):
        portal.patch_case_system_status("https://api", "qlin", "tok", [case])
    requests_mock.patch.assert_not_called()


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
