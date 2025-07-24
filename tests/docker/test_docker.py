import pytest
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="session")
def docker_container():
    import time

    _IMAGE_NAME = "radiant-vcf-operator:latest"
    with DockerContainer(_IMAGE_NAME).with_command("sleep 20") as container:
        time.sleep(3)
        yield container


def test_docker_image_contains_cyvcf(docker_container):
    exit_code, output = docker_container.exec("pip freeze")
    assert exit_code == 0
    assert b"cyvcf2" in output
    assert b"wurlitzer" in output
