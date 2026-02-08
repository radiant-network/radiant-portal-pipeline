import os
from unittest.mock import patch
from radiant.dags.operators.k8s import BaseK8SOperator


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
        context = BaseK8SOperator._get_k8s_context("my-iceberg-namespace")

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
