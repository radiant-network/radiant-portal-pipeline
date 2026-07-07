import sys
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from radiant.tasks.utils import S3DownloadError, capture_libc_stderr_and_check_errors, download_s3_file


def test_raises_value_error_when_error_pattern_is_found_in_stderr():
    error_patterns = ["error", "failure"]
    with patch("radiant.tasks.utils.pipes") as mock_pipes:
        mock_pipes.return_value.__enter__.return_value = (StringIO(""), StringIO("Some error occurred"))

        @capture_libc_stderr_and_check_errors(error_patterns)
        def faulty_function():
            pass

        try:
            faulty_function()
        except ValueError as e:
            assert "Detected error: Some error occurred" in str(e)


def test_executes_function_successfully_when_no_error_pattern_in_stderr():
    error_patterns = ["error", "failure"]
    with patch("radiant.tasks.utils.pipes") as mock_pipes:
        mock_pipes.return_value.__enter__.return_value = (StringIO(""), StringIO("All good"))

        @capture_libc_stderr_and_check_errors(error_patterns)
        def successful_function():
            return "Success"

        result = successful_function()
        assert result == "Success"


def test_handles_exception_and_flushes_pipes_correctly(capsys):
    error_patterns = ["error", "failure"]
    with patch("radiant.tasks.utils.pipes") as mock_pipes:
        mock_pipes.return_value.__enter__.return_value = (StringIO("stdout content"), StringIO("stderr content"))

        @capture_libc_stderr_and_check_errors(error_patterns)
        def exception_raising_function():
            print("Exception raised", file=sys.stderr)
            raise RuntimeError("Unexpected error")

        try:
            exception_raising_function()
        except RuntimeError as e:
            assert str(e) == "Unexpected error"
            mock_pipes.return_value.__exit__.assert_called()
            capture = capsys.readouterr()
            assert capture.out == "stdout content\n"
            assert capture.err == "Exception raised\nstderr content\n"


def test_download_s3_file_returns_local_path_on_success():
    mock_client = MagicMock()
    with patch("radiant.tasks.utils.boto3.client", return_value=mock_client):
        result = download_s3_file("s3://my-bucket/path/to/file.vcf.gz", "/tmp/dest")

    assert result == "/tmp/dest/file.vcf.gz"
    mock_client.download_file.assert_called_once_with("my-bucket", "path/to/file.vcf.gz", "/tmp/dest/file.vcf.gz")


def test_download_s3_file_propagates_error_with_s3_path():
    original = OSError("AccessDenied")
    mock_client = MagicMock()
    mock_client.download_file.side_effect = original
    s3_path = "s3://my-bucket/path/to/file.vcf.gz"

    with (
        patch("radiant.tasks.utils.boto3.client", return_value=mock_client),
        pytest.raises(S3DownloadError) as exc_info,
    ):
        download_s3_file(s3_path, "/tmp/dest")

    assert s3_path in str(exc_info.value)
    assert exc_info.value.__cause__ is original


def test_download_s3_file_randomizes_filename():
    mock_client = MagicMock()
    with patch("radiant.tasks.utils.boto3.client", return_value=mock_client):
        result = download_s3_file("s3://my-bucket/path/file.vcf.gz", "/tmp/dest", randomize_filename=True)

    assert result.startswith("/tmp/dest/")
    assert result.endswith("_file.vcf.gz")
    assert result != "/tmp/dest/file.vcf.gz"
