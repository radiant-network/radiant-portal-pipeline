import os
from types import SimpleNamespace
from unittest.mock import patch

from radiant.dags.operators.ecs import Toolbox


def test_get_run_command():
    fake_env = {
        "RADIANT_TOOLBOX_TASK_DEFINITION": "radiant-toolbox",
        "RADIANT_TOOLBOX_LOG_GROUP": "apps-qa/radiant-etl",
        "RADIANT_TOOLBOX_LOG_REGION": "us-east-1",
        "RADIANT_TOOLBOX_LOG_PREFIX": "ecs/radiant-toolbox-container",
    }
    ecs_env = SimpleNamespace(
        ECS_CLUSTER="my-cluster",
        ECS_SUBNETS=["subnet-1", "subnet-2"],
        ECS_SECURITY_GROUPS=["sg-1"],
    )
    with patch.dict(os.environ, fake_env, clear=True):
        op = Toolbox.get_run_command(ecs_env=ecs_env)

    assert op.task_id == "run_toolbox_command"
    assert op.cluster == "my-cluster"
    assert op.launch_type == "FARGATE"
    assert op.task_definition == "radiant-toolbox"
    assert op.aws_conn_id == "aws_default"

    assert op.awslogs_group == "apps-qa/radiant-etl"
    assert op.awslogs_region == "us-east-1"
    assert op.awslogs_stream_prefix == "ecs/radiant-toolbox-container"

    (container,) = op.overrides["containerOverrides"]
    assert container["name"] == "radiant-toolbox-container"
    assert container["command"] == "{{ [params.command] + (params.args or []) }}"
    # No extra_env given, defaults to an empty override rather than None.
    assert container["environment"] == []

    vpc = op.network_configuration["awsvpcConfiguration"]
    assert vpc["subnets"] == ["subnet-1", "subnet-2"]
    assert vpc["securityGroups"] == ["sg-1"]


def test_get_run_command_passes_through_extra_env():
    ecs_env = SimpleNamespace(ECS_CLUSTER="my-cluster", ECS_SUBNETS=[], ECS_SECURITY_GROUPS=[])
    with patch.dict(os.environ, {}, clear=True):
        op = Toolbox.get_run_command(ecs_env=ecs_env, extra_env=[{"name": "USER_PASSWORD", "value": "s3cr3t"}])

    (container,) = op.overrides["containerOverrides"]
    assert container["environment"] == [{"name": "USER_PASSWORD", "value": "s3cr3t"}]
