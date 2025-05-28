import sys
from io import StringIO
from unittest.mock import patch

from radiant.tasks.utils import capture_libc_stderr_and_check_errors


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
