from functools import wraps

from wurlitzer import pipes


def capture_libc_stderr_and_check_errors(error_patterns: list[str]):
    """
    A decorator to capture stderr output and check for specific error patterns.
    Raises a ValueError if the error pattern is found in the stderr output.

    :param error_pattern: The pattern to search for in stderr output.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with pipes() as (_, stderr):
                result = func(*args, **kwargs)
            errors = stderr.getvalue()
            if any(pattern in errors for pattern in error_patterns):
                raise ValueError(f"Detected error: {errors}")
            return result

        return wrapper

    return decorator
