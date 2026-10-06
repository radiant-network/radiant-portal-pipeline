import io
import json
import logging
from unittest.mock import MagicMock

import pytest
import requests
from airflow.exceptions import AirflowFailException

from radiant.tasks.gene_panel import upload
from radiant.tasks.nextflow.portal import PortalError
from radiant.tasks.nextflow.register import PortalConnection

CONN = PortalConnection(host="https://api", token_url="https://kc/token", client_id="airflow", client_secret="s")
FILEPATH = "s3://bucket/gene_panels/radiant/gene_panels.tsv"
CONTENT = b"panel_code\tpanel_name\tsymbol\nPANEL_1\tCardio\tTTN\n"


@pytest.fixture
def put(monkeypatch):
    monkeypatch.setattr(upload, "load_portal_connection", lambda conn_id: CONN)
    monkeypatch.setattr(PortalConnection, "token", lambda self: "tok")
    monkeypatch.setattr(upload, "read_s3_file", lambda uri: CONTENT)
    mocked = MagicMock(return_value={"panels": 1, "genes": 1, "warnings": []})
    monkeypatch.setattr(upload, "put_gene_panels", mocked)
    return mocked


def _refusal(status: int, detail=None) -> PortalError:
    body = json.dumps({"status": status, "message": "no", "detail": detail})
    return PortalError(f"PUT failed: {status} {body}", status=status, body=body)


def test_the_s3_file_goes_to_one_put_unchanged(put):
    upload.upload_gene_panels("radiant", FILEPATH, strict=True)

    put.assert_called_once_with("https://api", "radiant", "tok", "gene_panels.tsv", CONTENT, True)


def test_warnings_are_logged_and_counted(put, caplog):
    put.return_value = {
        "panels": 2,
        "genes": 40,
        "warnings": [{"line": 7, "panel_code": "PANEL_1", "symbol": "NOPE1", "message": "unknown gene"}],
    }

    with caplog.at_level(logging.INFO, logger=upload.LOGGER.name):
        summary = upload.upload_gene_panels("radiant", FILEPATH, strict=False)

    assert summary == {"panels": 2, "genes": 40, "warnings": 1}
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == ["line 7, panel PANEL_1, symbol NOPE1: unknown gene"]


def test_a_strict_rejection_logs_the_rows_and_fails_without_retry(put, caplog):
    rows = [{"line": 3, "panel_code": "P", "symbol": "X", "message": "unknown gene"}]
    put.side_effect = _refusal(422, {"warnings": rows})

    with caplog.at_level(logging.WARNING, logger=upload.LOGGER.name), pytest.raises(AirflowFailException) as e:
        upload.upload_gene_panels("radiant", FILEPATH, strict=True)

    assert "422" in str(e.value)
    assert [r.getMessage() for r in caplog.records] == ["line 3, panel P, symbol X: unknown gene"]


@pytest.mark.parametrize(
    "status, hint",
    [
        (400, "'line': 4"),
        (403, "can_manage_analysis_catalog"),
        (404, "not deployed"),
        (409, "panel_code"),
        (413, "10 MiB"),
    ],
)
def test_a_client_error_fails_without_retry(put, status, hint):
    put.side_effect = _refusal(status, {"line": 4})

    with pytest.raises(AirflowFailException) as e:
        upload.upload_gene_panels("radiant", FILEPATH, strict=False)

    assert str(status) in str(e.value)
    assert hint in str(e.value)


@pytest.mark.parametrize(
    "error", [_refusal(429), _refusal(500), _refusal(503), requests.ConnectionError("reset"), requests.Timeout()]
)
def test_a_server_or_network_error_is_raised_for_airflow_to_retry(put, error):
    put.side_effect = error

    with pytest.raises(type(error)) as e:
        upload.upload_gene_panels("radiant", FILEPATH, strict=False)

    assert not isinstance(e.value, AirflowFailException)


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("s3://bucket/key.tsv", ("bucket", "key.tsv")),
        ("s3://bucket/a/b/c.tsv", ("bucket", "a/b/c.tsv")),
        ("s3://bucket/panels#v2?.tsv", ("bucket", "panels#v2?.tsv")),
    ],
)
def test_parse_s3_uri(uri, expected):
    assert upload.parse_s3_uri(uri) == expected


@pytest.mark.parametrize("uri", ["", "bucket/key.tsv", "https://bucket/key.tsv", "s3://bucket", "s3://bucket/dir/"])
def test_parse_s3_uri_rejects_what_is_not_a_file(uri):
    with pytest.raises(ValueError):
        upload.parse_s3_uri(uri)


class _S3:
    """Stands in for the boto3 S3 client: one object, or one error on HEAD."""

    def __init__(self, body: bytes = b"", size: int | None = None, error: str | None = None):
        self.body = body
        self.size = len(body) if size is None else size
        self.error = error
        self.calls: list[tuple[str, dict]] = []

    def head_object(self, **kwargs):
        from botocore.exceptions import ClientError

        self.calls.append(("head_object", kwargs))
        if self.error:
            raise ClientError({"Error": {"Code": self.error, "Message": "x"}}, "HeadObject")
        return {"ContentLength": self.size}

    def get_object(self, **kwargs):
        self.calls.append(("get_object", kwargs))
        return {"Body": io.BytesIO(self.body)}


@pytest.fixture
def s3(monkeypatch):
    def install(client: _S3) -> _S3:
        monkeypatch.setattr("boto3.client", lambda service: client)
        return client

    return install


def test_read_s3_file_reads_the_object_of_the_uri(s3):
    client = s3(_S3(body=CONTENT))

    assert upload.read_s3_file(FILEPATH) == CONTENT
    assert client.calls == [
        ("head_object", {"Bucket": "bucket", "Key": "gene_panels/radiant/gene_panels.tsv"}),
        ("get_object", {"Bucket": "bucket", "Key": "gene_panels/radiant/gene_panels.tsv"}),
    ]


def test_read_s3_file_refuses_a_file_larger_than_the_portal_accepts_before_downloading(s3):
    client = s3(_S3(size=upload.MAX_FILE_BYTES + 1))

    with pytest.raises(AirflowFailException, match="more than"):
        upload.read_s3_file(FILEPATH)

    assert [name for name, _ in client.calls] == ["head_object"]


@pytest.mark.parametrize("code", ["404", "403", "NoSuchBucket"])
def test_read_s3_file_fails_without_retry_when_the_object_is_missing_or_unreadable(s3, code):
    s3(_S3(error=code))

    with pytest.raises(AirflowFailException, match=f"cannot read {FILEPATH}: {code}"):
        upload.read_s3_file(FILEPATH)


def test_read_s3_file_raises_other_s3_errors_for_airflow_to_retry(s3):
    from botocore.exceptions import ClientError

    s3(_S3(error="SlowDown"))

    with pytest.raises(ClientError):
        upload.read_s3_file(FILEPATH)
