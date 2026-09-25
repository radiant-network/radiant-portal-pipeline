import os
from types import SimpleNamespace
from unittest.mock import patch

from radiant.dags.operators.ecs import CosmicMutationSet


def test_get_normalize():
    fake_env = {
        "RADIANT_TASK_OPERATOR_TASK_DEFINITION": "radiant-operator",
        "RADIANT_TASK_OPERATOR_LOG_GROUP": "apps-qa/radiant-etl",
        "RADIANT_TASK_OPERATOR_LOG_REGION": "us-east-1",
        "RADIANT_TASK_OPERATOR_LOG_PREFIX": "ecs/radiant-operator-qa-etl-container",
    }
    ecs_env = SimpleNamespace(ECS_CLUSTER="my-cluster", ECS_SUBNETS=["subnet-1"], ECS_SECURITY_GROUPS=["sg-1"])
    with patch.dict(os.environ, fake_env, clear=True):
        op = CosmicMutationSet.get_normalize(ecs_env=ecs_env)

    assert op.task_id == "normalize_cosmic_mutation_set_ecs"
    assert op.cluster == "my-cluster"
    assert op.launch_type == "FARGATE"
    # Same task definition (and image) as the VCF extraction: bcftools ships in the radiant-operator image.
    assert op.task_definition == "radiant-operator"
    assert op.awslogs_group == "apps-qa/radiant-etl"
    assert op.awslogs_stream_prefix == "ecs/radiant-operator-qa-etl-container"
    assert op.aws_conn_id == "aws_default"

    (container,) = op.overrides["containerOverrides"]
    assert container["name"] == "radiant-operator-qa-etl-container"
    (command,) = container["command"]
    assert command.startswith("python /opt/radiant/normalize_cosmic_mutation_set.py ")
    assert "--input '{{ params.cosmic_mutation_set_filepath }}'" in command
    assert "--fasta '{{ params.reference_fasta_filepath }}'" in command
    assert "--output '{{ ti.xcom_pull(task_ids=\"resolve_normalized_filepath\") }}'" in command
    env = {e["name"]: e["value"] for e in container["environment"]}
    assert env["PYTHONPATH"] == "/opt/radiant"
    assert env["STARROCKS_BROKER_USE_INSTANCE_PROFILE"] == "true"

    vpc = op.network_configuration["awsvpcConfiguration"]
    assert vpc["subnets"] == ["subnet-1"] and vpc["securityGroups"] == ["sg-1"]
