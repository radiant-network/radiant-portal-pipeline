from testcontainers.core.container import DockerContainer


def test_docker_image_contains_cyvcf():
    _IMAGE_NAME = "ghcr.io/radiant-network/radiant-airflow-task-operator:latest"
    with DockerContainer(_IMAGE_NAME).with_command("sleep 10") as container:
        exit_code, output = container.exec('/bin/bash -c "python -m pip freeze"')
        assert exit_code == 0
        assert b"cyvcf2" in output
        assert b"wurlitzer" in output


def test_docker_image_contains_bcftools():
    _IMAGE_NAME = "ghcr.io/radiant-network/radiant-airflow-task-operator:latest"
    with DockerContainer(_IMAGE_NAME).with_command("sleep 10") as container:
        exit_code, output = container.exec("bcftools --version")
        assert exit_code == 0
        assert output.startswith(b"bcftools 1.")


def test_docker_image_loads_the_rust_snv_engine():
    """The wheel is cross-compiled for linux/amd64: check that it loads in the image, not only that it installs."""
    _IMAGE_NAME = "ghcr.io/radiant-network/radiant-airflow-task-operator:latest"
    with DockerContainer(_IMAGE_NAME).with_command("sleep 10") as container:
        exit_code, output = container.exec(
            'python -c "import radiant_vcf; print(radiant_vcf.GermlineSnvReader, radiant_vcf.SomaticSnvReader)"'
        )
        assert exit_code == 0, output
        assert b"GermlineSnvReader" in output
