import os
from types import SimpleNamespace
from unittest.mock import patch

from radiant.dags.operators.ecs import IgvTracks


def test_get_build_track():
    fake_env = {
        "RADIANT_TASK_OPERATOR_TASK_DEFINITION": "radiant-operator",
        "RADIANT_TASK_OPERATOR_LOG_GROUP": "apps-qa/radiant-etl",
        "RADIANT_TASK_OPERATOR_LOG_REGION": "us-east-1",
        "RADIANT_TASK_OPERATOR_LOG_PREFIX": "ecs/radiant-operator-mock-etl-container",
        "RADIANT_ENVIRONMENT": "mock",
    }
    ecs_env = SimpleNamespace(ECS_CLUSTER="my-cluster", ECS_SUBNETS=["subnet-1"], ECS_SECURITY_GROUPS=["sg-1"])
    with patch.dict(os.environ, fake_env, clear=True):
        partial = IgvTracks.get_build_track(ecs_env=ecs_env, ephemeral_storage_gib=42)

    kwargs = partial.kwargs
    assert kwargs["task_id"] == "build_igv_track_ecs"
    assert kwargs["cluster"] == "my-cluster"
    assert kwargs["launch_type"] == "FARGATE"
    # Same task definition (and image) as the VCF extraction: bgzip and tabix ship in the radiant-operator image.
    assert kwargs["task_definition"] == "radiant-operator"
    assert kwargs["retries"] == 2
    assert kwargs["max_active_tis_per_dagrun"] == 4
    assert kwargs["map_index_template"] == "{{ params.track_id }}"

    (container,) = kwargs["overrides"]["containerOverrides"]
    # Named after RADIANT_ENVIRONMENT, like the terraform task definition: a fixed name breaks every other env.
    assert container["name"] == "radiant-operator-mock-etl-container"
    (command,) = container["command"]
    assert command.startswith("python /opt/radiant/build_igv_track.py ")
    assert "--track_id '{{ params.track_id }}'" in command
    assert "--prefix '{{ params.igv_tracks_prefix }}'" in command
    env = {e["name"]: e["value"] for e in container["environment"]}
    assert env["PYTHONPATH"] == "/opt/radiant"
    # The shared task definition is not sized for chainSelf's sort spill: the task sets the disk the DAG asks for.
    assert kwargs["overrides"]["ephemeralStorage"] == {"sizeInGiB": 42}
