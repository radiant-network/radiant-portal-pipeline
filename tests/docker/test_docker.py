from testcontainers.core.container import DockerContainer


def test_docker_image_contains_cyvcf():
    _IMAGE_NAME = "ghcr.io/radiant-network/radiant-airflow-task-operator:latest"
    with DockerContainer(_IMAGE_NAME).with_command("sleep 10") as container:
        exit_code, output = container.exec('/bin/bash -c "python -m pip freeze"')
        assert exit_code == 0
        assert b"cyvcf2" in output
        assert b"wurlitzer" in output
