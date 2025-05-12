import platform

import pytest


@pytest.fixture(scope="session")
def host_internal_address():
    """Detects the OS and returns the appropriate host IP for Docker networking."""
    os_name = platform.system()

    if os_name == "Darwin" or os_name == "Windows":
        yield "host.docker.internal"  # Standard for macOS/Windows Docker Desktop
    elif os_name == "Linux":
        yield "172.17.0.1"  # Default Docker bridge gateway IP for Linux
    else:
        raise OSError(f"Unsupported operating system: {os_name}")
