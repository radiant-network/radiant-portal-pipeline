import os
from unittest.mock import patch

from radiant.dags.operators.k8s import CheckDataIntegrity


def test_get_run_dbt():
    fake_env = {
        "RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE": "airflow",
        "RADIANT_TASK_OPERATOR_SERVICE_ACCOUNT_NAME": "airflow-sa",
        "RADIANT_DBT_OPERATOR_IMAGE": "my-registry/radiant-dbt:test",
        "AWS_REGION": "ca-central-1",
        "AWS_ACCESS_KEY_ID": "my-access-key",
        "AWS_SECRET_ACCESS_KEY": "my-secret-key",
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_ALLOW_HTTP": "true",
    }
    with patch.dict(os.environ, fake_env, clear=True):
        op = CheckDataIntegrity.get_run_dbt(
            run_results_s3_uri="s3://bucket/dbt-qa/run1/run_results.json",
            junit_s3_uri="s3://bucket/dbt-qa/run1/junit.xml",
            tenants='[{"code": "chusj", "schema": "chusj_tenant"}]',
        )

    assert op.task_id == "run_dbt"
    assert op.namespace == "airflow"
    assert op.service_account_name == "airflow-sa"
    assert op.image == "my-registry/radiant-dbt:test"
    assert op.deferrable is False
    assert op.get_logs is True

    # StarRocks connection injected from the existing K8s secret.
    (secret,) = op.secrets
    assert secret.deploy_type == "env"
    assert secret.deploy_target == "AIRFLOW_CONN_STARROCKS_CONN"
    assert secret.secret == "starrocks-airflow-conn"
    assert secret.key == "AIRFLOW_CONN_STARROCKS_CONN"

    # verify env vars are injected. The AWS creds (key/secret/endpoint/http) are
    # for the local sandbox only — empty in prod (EKS) → boto3 uses Pod Identity.
    env = {e.name: e.value for e in op.env_vars}
    assert env["RUN_RESULTS_S3_URI"] == "s3://bucket/dbt-qa/run1/run_results.json"
    assert env["JUNIT_S3_URI"] == "s3://bucket/dbt-qa/run1/junit.xml"
    # Drives the per-tenant dbt passes inside the container.
    assert env["TENANTS"] == '[{"code": "chusj", "schema": "chusj_tenant"}]'
    assert env["AWS_REGION"] == "ca-central-1"
    assert env["AWS_ACCESS_KEY_ID"] == "my-access-key"
    assert env["AWS_SECRET_ACCESS_KEY"] == "my-secret-key"
    assert env["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert env["AWS_ALLOW_HTTP"] == "true"
