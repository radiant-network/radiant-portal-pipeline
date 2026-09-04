import os
from unittest.mock import patch

from radiant.dags.operators.k8s import Toolbox


def test_get_run_command():
    fake_env = {
        "RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE": "airflow",
        "RADIANT_TASK_OPERATOR_SERVICE_ACCOUNT_NAME": "airflow-sa",
        "RADIANT_TOOLBOX_OPERATOR_IMAGE": "my-registry/radiant-toolbox:test",
    }
    with patch.dict(os.environ, fake_env, clear=True):
        op = Toolbox.get_run_command()

    assert op.task_id == "run_toolbox_command"
    assert op.namespace == "airflow"
    assert op.service_account_name == "airflow-sa"
    assert op.image == "my-registry/radiant-toolbox:test"
    assert op.cmds == ["{{ params.command }}"]
    assert op.arguments == "{{ params.args }}"
    assert op.deferrable is False
    assert op.get_logs is True

    # Whole secret exposed as env vars (no single key), unlike CheckDataIntegrity's
    # single-key StarRocks conn secret.
    (secret,) = op.secrets
    assert secret.deploy_type == "env"
    assert secret.deploy_target is None
    assert secret.secret == "radiant-toolbox-secret"


def test_get_run_command_honours_custom_secret_name():
    fake_env = {"RADIANT_TOOLBOX_OPERATOR_SECRET_NAME": "custom-toolbox-secret"}
    with patch.dict(os.environ, fake_env, clear=True):
        op = Toolbox.get_run_command()

    (secret,) = op.secrets
    assert secret.secret == "custom-toolbox-secret"


def test_get_run_command_passes_through_extra_env():
    with patch.dict(os.environ, {}, clear=True):
        op = Toolbox.get_run_command(extra_env={"USER_PASSWORD": "s3cr3t"})

    # KubernetesPodOperator normalizes a dict env_vars into a list of V1EnvVar at init.
    (env,) = op.env_vars
    assert (env.name, env.value) == ("USER_PASSWORD", "s3cr3t")


def test_get_run_command_defaults_extra_env_to_none():
    with patch.dict(os.environ, {}, clear=True):
        op = Toolbox.get_run_command()

    assert op.env_vars == []
