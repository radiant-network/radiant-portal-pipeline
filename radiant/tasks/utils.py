import sys
from functools import wraps

from wurlitzer import pipes
from urllib import parse

import boto3
import os

def _flush_pipes(stdout, stderr):
    """
    Flushes the logs to the specified file.
    """
    _stdout = stdout.getvalue()
    if _stdout:
        print(_stdout, file=sys.stdout, flush=True)
    _stderr = stderr.getvalue()
    if _stderr:
        print(_stderr, file=sys.stderr, flush=True)
    return _stdout, _stderr


def capture_libc_stderr_and_check_errors(error_patterns: list[str]):
    """
    A decorator to capture stderr output and check for specific error patterns.
    Raises a ValueError if the error pattern is found in the stderr output.

    :param error_pattern: The pattern to search for in stderr output.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _exception = None
            with pipes() as (stdout, stderr):
                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    _exception = e

            _, errors = _flush_pipes(stdout, stderr)
            if _exception:
                raise _exception
            if any(pattern in errors for pattern in error_patterns):
                raise ValueError(f"Detected error: {errors}")

            return result

        return wrapper

    return decorator

def download_s3_file(s3_path, dest_dir, randomize_filename=False):
    s3_client = boto3.client("s3")

    def extract_bucket_key(s3_path):
        parsed = parse.urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    bucket_name, object_key = extract_bucket_key(s3_path)
    if randomize_filename:
        import uuid
        filename = f"{uuid.uuid4()}_{os.path.basename(object_key)}"
    else:
        filename  = os.path.basename(object_key)
    local_path = os.path.join(dest_dir, filename)
    try:
        s3_client.download_file(bucket_name, object_key, local_path)
    except Exception as e:
        print(f"Error downloading S3 file: {e}")
        return None
    return local_path