import os
from types import SimpleNamespace
from unittest.mock import patch

from radiant.dags.operators.ecs import CheckDataIntegrity


def test_get_run_dbt():
    fake_env = {
        "RADIANT_DBT_TASK_DEFINITION": "radiant-dbt",
        "RADIANT_DBT_LOG_GROUP": "apps-qa/radiant-etl",
        "RADIANT_DBT_LOG_REGION": "us-east-1",
        "RADIANT_DBT_LOG_PREFIX": "ecs/radiant-dbt-container",
    }
    ecs_env = SimpleNamespace(
        ECS_CLUSTER="my-cluster",
        ECS_SUBNETS=["subnet-1", "subnet-2"],
        ECS_SECURITY_GROUPS=["sg-1"],
    )
    with patch.dict(os.environ, fake_env, clear=True):
        op = CheckDataIntegrity.get_run_dbt(
            run_results_s3_uri="s3://bucket/dbt-qa/run1/run_results.json",
            junit_s3_uri="s3://bucket/dbt-qa/run1/junit.xml",
            ecs_env=ecs_env,
        )

    assert op.task_id == "run_dbt"
    assert op.cluster == "my-cluster"
    assert op.launch_type == "FARGATE"
    assert op.task_definition == "radiant-dbt"
    assert op.aws_conn_id == "aws_default"

    # awslogs config
    assert op.awslogs_group == "apps-qa/radiant-etl"
    assert op.awslogs_region == "us-east-1"
    assert op.awslogs_stream_prefix == "ecs/radiant-dbt-container"

    # container name must match the override target
    (container,) = op.overrides["containerOverrides"]
    assert container["name"] == "radiant-dbt-container"

    # verify per-run S3 URIs are injected via containerOverrides
    env = {e["name"]: e["value"] for e in container["environment"]}
    assert env["RUN_RESULTS_S3_URI"] == "s3://bucket/dbt-qa/run1/run_results.json"
    assert env["JUNIT_S3_URI"] == "s3://bucket/dbt-qa/run1/junit.xml"

    # network config from ecs_env
    vpc = op.network_configuration["awsvpcConfiguration"]
    assert vpc["subnets"] == ["subnet-1", "subnet-2"]
    assert vpc["securityGroups"] == ["sg-1"]
