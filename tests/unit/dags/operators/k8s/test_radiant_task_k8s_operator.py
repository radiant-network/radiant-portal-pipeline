import os
from unittest.mock import patch

from radiant.dags.operators.k8s import (
    RadiantTaskK8SOperator,
    _cnv_container_resources,
    _container_resources,
    _metadata_container_resources,
    _snv_container_resources,
)


def test_get_k8s_context():
    fake_env = {
        "PYICEBERG_CATALOG__DEFAULT__FOO": "foo-value",
        "PYICEBERG_CATALOG__DEFAULT__BAR": "bar-value",
        "AWS_REGION": "my-aws-region",
        "AWS_ACCESS_KEY_ID": "my-access-key-id",
        "AWS_SECRET_ACCESS_KEY": "my-secret-access-key",
        "AWS_ENDPOINT_URL": "https://my-aws-endpoint-url",
        "AWS_ALLOW_HTTP": "true",
        "RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE": "my-kubernetes-namespace",
        "RADIANT_TASK_OPERATOR_IMAGE": "test-image",
        "RADIANT_TASK_OPERATOR_PYTHONPATH": "/code",
        "RADIANT_TASK_OPERATOR_LD_LIBRARY_PATH": "/lib",
    }
    with patch.dict(os.environ, fake_env, clear=True):
        context = RadiantTaskK8SOperator._get_k8s_context("my-iceberg-namespace")

    assert context["namespace"] == "my-kubernetes-namespace"
    assert context["image"] == "test-image"
    assert context["image_pull_policy"] == "IfNotPresent"
    assert context["get_logs"] is True
    assert context["is_delete_operator_pod"] is True

    env_vars = context["env_vars"]
    assert env_vars["AWS_REGION"] == "my-aws-region"
    assert env_vars["AWS_ACCESS_KEY_ID"] == "my-access-key-id"
    assert env_vars["AWS_SECRET_ACCESS_KEY"] == "my-secret-access-key"
    assert env_vars["AWS_ENDPOINT_URL"] == "https://my-aws-endpoint-url"
    assert env_vars["AWS_ALLOW_HTTP"] == "true"
    assert env_vars["RADIANT_ICEBERG_NAMESPACE"] == "my-iceberg-namespace"
    assert env_vars["PYTHONPATH"] == "/code"
    assert env_vars["LD_LIBRARY_PATH"] == "/lib"
    assert env_vars["PYICEBERG_CATALOG__DEFAULT__FOO"] == "foo-value"
    assert env_vars["PYICEBERG_CATALOG__DEFAULT__BAR"] == "bar-value"

    # Unsized tasks must not gain an empty `container_resources`, which would override
    # any value the caller passes on the left of the `dict | dict` merge.
    assert "container_resources" not in context


def test_get_k8s_context_with_container_resources():
    with patch.dict(os.environ, {}, clear=True):
        resources = _snv_container_resources()
        context = RadiantTaskK8SOperator._get_k8s_context("my-iceberg-namespace", container_resources=resources)

    assert context["container_resources"] is resources


def test_profiles_are_sized_separately():
    """SNV holds three accumulator buffers at once; CNV needs a fraction of that; and
    committing partitions is metadata-only, so it scales with file count, not VCF size.
    """
    with patch.dict(os.environ, {}, clear=True):
        snv = _snv_container_resources()
        cnv = _cnv_container_resources()
        metadata = _metadata_container_resources()

    assert snv.requests == {"cpu": "1", "memory": "4Gi"}
    # A CPU limit would throttle cyvcf2 and the parallel parquet writes.
    assert snv.limits == {"memory": "6Gi"}

    assert cnv.requests == {"cpu": "1", "memory": "500Mi"}
    assert cnv.limits == {"memory": "1Gi"}

    # Footer reads are sequential and network-bound, so half a core is enough.
    assert metadata.requests == {"cpu": "500m", "memory": "1Gi"}
    assert metadata.limits == {"memory": "2Gi"}


def test_container_resources_profile_is_env_overridable():
    overrides = {
        "RADIANT_TASK_OPERATOR_SNV_CPU": "4",
        "RADIANT_TASK_OPERATOR_SNV_MEMORY": "16Gi",
        "RADIANT_TASK_OPERATOR_SNV_MEMORY_LIMIT": "24Gi",
    }
    with patch.dict(os.environ, overrides, clear=True):
        snv = _snv_container_resources()
        # A different profile must not pick up the SNV overrides.
        cnv = _cnv_container_resources()

    assert snv.requests == {"cpu": "4", "memory": "16Gi"}
    assert snv.limits == {"memory": "24Gi"}
    assert cnv.requests == {"cpu": "1", "memory": "500Mi"}


def test_container_resources_uses_defaults_when_env_absent():
    with patch.dict(os.environ, {}, clear=True):
        resources = _container_resources("WHATEVER", cpu="2", memory="8Gi", memory_limit="8Gi")

    assert resources.requests == {"cpu": "2", "memory": "8Gi"}
    assert resources.limits == {"memory": "8Gi"}
