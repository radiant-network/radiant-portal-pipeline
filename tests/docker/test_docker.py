import pytest
from testcontainers.core.container import DockerContainer, wait_for_logs


@pytest.fixture(scope="session")
def docker_container():
    _IMAGE_NAME = "radiant-airflow:latest"
    with DockerContainer(_IMAGE_NAME).with_command("standalone") as container:
        wait_for_logs(container, "standalone | Starting Airflow Standalone", timeout=60)
        yield container


def test_docker_image_contains_cyvcf(docker_container):
    exit_code, output = docker_container.exec('/bin/bash -c "/home/airflow/.venv/radiant/bin/pip freeze"')
    assert exit_code == 0
    assert b"cyvcf2" in output
    assert b"wurlitzer" in output
