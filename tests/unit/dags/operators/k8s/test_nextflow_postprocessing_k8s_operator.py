import os
from unittest.mock import patch

from radiant.dags.operators.k8s import NextflowPostprocessing


def _build(env: dict):
    with patch.dict(os.environ, env, clear=True):
        return NextflowPostprocessing.get_run_postprocessing(
            input_csv="/workspace/inputs/samplesheet.csv",
            outdir="/workspace/outputs/qlin/run1",
            run_tag="manual__2026-08-20T14-30-00-00-00",
        )


def test_get_run_postprocessing_with_env():
    fake_env = {
        "NEXTFLOW_OPERATOR_IMAGE": "my-registry/nextflow-launcher:test",
        "NEXTFLOW_OPERATOR_KUBERNETES_NAMESPACE": "nf",
        "NEXTFLOW_OPERATOR_SERVICE_ACCOUNT_NAME": "nf-sa",
        "NEXTFLOW_OPERATOR_PVC_NAME": "fsx-test",
        "NEXTFLOW_OPERATOR_NODEPOOL": "test-pool",
        "NEXTFLOW_OPERATOR_WORKSPACE_PATH": "/ws",
        "RADIANT_TASK_OPERATOR_NEXTFLOW_MEMORY": "8Gi",
    }
    op = _build(fake_env)

    assert op.task_id == "run_postprocessing"
    assert op.namespace == "nf"
    assert op.service_account_name == "nf-sa"
    assert op.image == "my-registry/nextflow-launcher:test"

    # Deferrable so a multi-hour run does not hold an Airflow worker slot.
    assert op.deferrable is True
    assert op.get_logs is True
    assert op.node_selector == {"nodepool": "test-pool"}
    assert op.annotations == {"karpenter.sh/do-not-disrupt": "true"}

    # The env-var override path on the shared _container_resources helper.
    assert op.container_resources.requests["memory"] == "8Gi"

    volumes = {v.name: v for v in op.volumes}
    assert set(volumes) == {"workspace", "nextflow-cfg", "nextflow-params"}
    assert volumes["workspace"].persistent_volume_claim.claim_name == "fsx-test"
    assert volumes["nextflow-cfg"].config_map.name == "nextflow-cfg"
    assert volumes["nextflow-params"].config_map.name == "nextflow-params"

    mounts = {m.name: m for m in op.volume_mounts}
    assert mounts["workspace"].mount_path == "/ws"
    assert mounts["nextflow-cfg"].mount_path == "/etc/nextflow"
    assert mounts["nextflow-cfg"].read_only is True
    assert mounts["nextflow-params"].read_only is True

    env = {e.name: e.value for e in op.env_vars}
    assert env["NXF_INPUT"] == "/workspace/inputs/samplesheet.csv"
    assert env["NXF_OUTDIR"] == "/workspace/outputs/qlin/run1"
    assert env["RUN_TAG"] == "manual__2026-08-20T14-30-00-00-00"
    assert env["NXF_WORKSPACE"] == "/ws"
    assert env["NXF_ANSI_LOG"] == "false"

    # Pod Identity provides the credentials; injecting empty AWS_* would break the
    # credential chain, so unlike the other operators none are set here.
    assert not [name for name in env if name.startswith("AWS_")]


def test_get_run_postprocessing_defaults_match_the_qa_topology():
    """With nothing configured, the operator still targets the real deployment --
    only the image has no sensible default."""
    op = _build({})

    assert op.namespace == "nextflow"
    assert op.service_account_name == "nextflow"
    assert op.node_selector == {"nodepool": "qlin-nextflow"}
    assert op.volumes[0].persistent_volume_claim.claim_name == "fsx-nextflow"
    assert op.volume_mounts[0].mount_path == "/workspace"
    assert op.container_resources.requests == {"cpu": "1", "memory": "2Gi"}
    assert op.container_resources.limits == {"memory": "4Gi"}

    # A missing image must fail loudly at pod creation rather than silently pull
    # something unintended.
    assert op.image is None


def test_startup_timeout_survives_a_karpenter_cold_start():
    """The provider default is 120s, which a node launch plus a multi-GB image pull
    exceeds -- the trigger would raise PodLaunchTimeoutException before the driver
    ever starts."""
    op = _build({})
    assert op.startup_timeout_seconds == 1800
    # Backstop for a cleared deferred task, whose pod is not deleted.
    assert op.active_deadline_seconds == 86400


def test_driver_script_is_jinja_safe():
    """cmds/arguments are templated fields: a stray {{ or {% in the launch script
    would fail DAG parsing, and the failure would look nothing like its cause."""
    (script,) = op_arguments = _build({}).arguments
    assert op_arguments  # sanity
    assert "{{" not in script
    assert "{%" not in script
    # The cd is what makes -resume work: Nextflow reads .nextflow/cache from cwd.
    assert 'cd "$LAUNCH"' in script
    assert "-resume" in script


def _cleanup(env: dict):
    with patch.dict(os.environ, env, clear=True):
        return NextflowPostprocessing.get_cleanup_work(run_tag="manual__2026-08-20T14-30-00-00-00")


def test_cleanup_targets_only_this_run():
    op = _cleanup({"NEXTFLOW_OPERATOR_IMAGE": "img:test"})
    assert op.task_id == "cleanup_work"
    assert op.namespace == "nextflow"
    env = {e.name: e.value for e in op.env_vars}
    assert env["RUN_TAG"] == "manual__2026-08-20T14-30-00-00-00"
    assert env["NXF_WORKSPACE"] == "/workspace"
    # Not deferrable: it is a few seconds of rm, not a multi-hour wait.
    assert op.deferrable is False
    # Mounts the PVC so it can delete scratch, and nothing else.
    assert [v.name for v in op.volumes] == ["workspace"]
    assert op.volumes[0].persistent_volume_claim.claim_name == "fsx-nextflow"


def test_cleanup_script_refuses_to_run_with_an_empty_run_tag():
    """An empty RUN_TAG would expand to /workspace/work/ and wipe every run's
    scratch on the shared filesystem, including a concurrently running job."""
    (script,) = _cleanup({}).arguments
    assert "${RUN_TAG:?" in script
    assert "${NXF_WORKSPACE:?" in script
    assert "set -euo pipefail" in script
    # Deletion must be scoped to the interpolated paths, never a bare prefix.
    assert 'rm -rf "$d"' in script
    assert "{{" not in script and "{%" not in script
