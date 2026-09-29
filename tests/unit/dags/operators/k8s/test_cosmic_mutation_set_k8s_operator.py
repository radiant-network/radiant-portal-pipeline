import os
from unittest.mock import patch

from radiant.dags.operators.k8s import CosmicMutationSet, _container_resources, _cosmic_container_resources


def test_cosmic_profile_requests_scratch_disk():
    """The task stages a 3 GB FASTA plus intermediates on the node, so it asks for ephemeral storage --
    the other profiles do not."""
    with patch.dict(os.environ, {}, clear=True):
        resources = _cosmic_container_resources()
    assert resources.requests == {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "8Gi"}
    assert resources.limits == {"memory": "4Gi", "ephemeral-storage": "8Gi"}

    with patch.dict(os.environ, {"RADIANT_TASK_OPERATOR_COSMIC_EPHEMERAL_STORAGE": "20Gi"}, clear=True):
        assert _cosmic_container_resources().requests["ephemeral-storage"] == "20Gi"

    plain = _container_resources("X", cpu="1", memory="1Gi", memory_limit="2Gi")
    assert "ephemeral-storage" not in plain.requests and "ephemeral-storage" not in plain.limits


def test_get_normalize():
    fake_env = {
        "RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE": "airflow",
        "RADIANT_TASK_OPERATOR_SERVICE_ACCOUNT_NAME": "airflow-sa",
        "RADIANT_TASK_OPERATOR_IMAGE": "my-registry/radiant-task-operator:test",
        "AWS_REGION": "ca-central-1",
    }
    with patch.dict(os.environ, fake_env, clear=True):
        factory = CosmicMutationSet.get_normalize("my-iceberg-namespace")
        op = factory(
            input_filepath="s3://bucket/cosmic/cmc_export.tsv.gz",
            reference_fasta_filepath="s3://bucket/reference/ref.fa",
            output_filepath="s3://bucket/cosmic/cmc_export.normalized.tsv.gz",
        ).operator

    assert op.task_id == "normalize_cosmic_mutation_set_k8s"
    assert op.namespace == "airflow"
    assert op.image == "my-registry/radiant-task-operator:test"
    assert op.service_account_name == "airflow-sa"
    assert op.do_xcom_push is True
    assert op.container_resources.requests["ephemeral-storage"] == "8Gi"

    env = {e.name: e.value for e in op.env_vars}
    assert env["RADIANT_ICEBERG_NAMESPACE"] == "my-iceberg-namespace"
    assert env["AWS_REGION"] == "ca-central-1"
    assert op.op_kwargs == {
        "input_filepath": "s3://bucket/cosmic/cmc_export.tsv.gz",
        "reference_fasta_filepath": "s3://bucket/reference/ref.fa",
        "output_filepath": "s3://bucket/cosmic/cmc_export.normalized.tsv.gz",
    }
