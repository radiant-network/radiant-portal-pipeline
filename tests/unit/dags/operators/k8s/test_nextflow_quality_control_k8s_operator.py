import os
from unittest.mock import patch

from radiant.dags.operators.k8s import NextflowQualityControl


def _build(env: dict):
    with patch.dict(os.environ, env, clear=True):
        return NextflowQualityControl.get_run_quality_control(
            input_csv="/workspace/inputs/qc/samplesheet.csv",
            outdir="/workspace/outputs/qlin/qc-run1",
            dragen_metrics_dir="/workspace/inputs/qc/metrics",
            run_tag="qc-manual__2026-08-20T14-30-00-00-00",
        )


def test_get_run_quality_control_with_env():
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

    assert op.task_id == "run_quality_control"
    assert op.namespace == "nf"
    assert op.service_account_name == "nf-sa"
    # Same driver image as post-processing: both pipelines are baked into it.
    assert op.image == "my-registry/nextflow-launcher:test"

    # Deferrable so a long run does not hold an Airflow worker slot.
    assert op.deferrable is True
    assert op.get_logs is True
    assert op.node_selector == {"nodepool": "test-pool"}
    assert op.annotations == {"karpenter.sh/do-not-disrupt": "true"}

    # The env-var override path on the shared _container_resources helper.
    assert op.container_resources.requests["memory"] == "8Gi"

    volumes = {v.name: v for v in op.volumes}
    assert set(volumes) == {"workspace", "nextflow-cfg", "nextflow-params"}
    assert volumes["workspace"].persistent_volume_claim.claim_name == "fsx-test"

    mounts = {m.name: m for m in op.volume_mounts}
    assert mounts["workspace"].mount_path == "/ws"
    # The mount paths are the same as post-processing's; only the ConfigMaps differ.
    assert mounts["nextflow-cfg"].mount_path == "/etc/nextflow"
    assert mounts["nextflow-cfg"].read_only is True
    assert mounts["nextflow-params"].mount_path == "/etc/nextflow-params"
    assert mounts["nextflow-params"].read_only is True

    env = {e.name: e.value for e in op.env_vars}
    assert env["NXF_INPUT"] == "/workspace/inputs/qc/samplesheet.csv"
    assert env["NXF_OUTDIR"] == "/workspace/outputs/qlin/qc-run1"
    assert env["NXF_DRAGEN_METRICS_DIR"] == "/workspace/inputs/qc/metrics"
    assert env["RUN_TAG"] == "qc-manual__2026-08-20T14-30-00-00-00"
    assert env["NXF_WORKSPACE"] == "/ws"
    assert env["NXF_ANSI_LOG"] == "false"

    # Pod Identity provides the credentials; injecting empty AWS_* would break the
    # credential chain, so unlike the other operators none are set here.
    assert not [name for name in env if name.startswith("AWS_")]


def test_qc_uses_its_own_configmaps():
    """The post-processing nextflow.config references params.save_genotyped and
    params.tools; a param referenced from a `-c` config but absent from the
    -params-file kills the run at config parse. Hence a separate pair."""
    op = _build({})
    volumes = {v.name: v for v in op.volumes}
    assert volumes["nextflow-cfg"].config_map.name == "nextflow-qc-cfg"
    assert volumes["nextflow-params"].config_map.name == "nextflow-qc-params"

    overridden = _build(
        {
            "NEXTFLOW_QC_OPERATOR_CONFIG_CONFIGMAP": "qc-cfg-test",
            "NEXTFLOW_QC_OPERATOR_PARAMS_CONFIGMAP": "qc-params-test",
        }
    )
    volumes = {v.name: v for v in overridden.volumes}
    assert volumes["nextflow-cfg"].config_map.name == "qc-cfg-test"
    assert volumes["nextflow-params"].config_map.name == "qc-params-test"


def test_get_run_quality_control_defaults_match_the_qa_topology():
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


def test_driver_script_is_jinja_safe_and_runs_the_qc_pipeline():
    """cmds/arguments are templated fields: a stray {{ or {% in the launch script
    would fail DAG parsing, and the failure would look nothing like its cause."""
    (script,) = _build({}).arguments
    assert "{{" not in script
    assert "{%" not in script
    # The cd is what makes -resume work: Nextflow reads .nextflow/cache from cwd.
    assert 'cd "$LAUNCH"' in script
    assert "-resume" in script
    # The QC pipeline asset, not the post-processing one.
    assert "assets/Ferlab-Ste-Justine/quality-control-pipeline" in script
    assert "Post-processing-Pipeline" not in script
    # Passing this flag is the whole point: it is what skips BAM_QC and VCF_QC.
    assert '--dragen_metrics_dir "$NXF_DRAGEN_METRICS_DIR"' in script


def _cleanup(env: dict):
    with patch.dict(os.environ, env, clear=True):
        return NextflowQualityControl.get_cleanup_work(run_tag="qc-manual__2026-08-20T14-30-00-00-00")


def test_cleanup_targets_only_this_run():
    op = _cleanup({"NEXTFLOW_OPERATOR_IMAGE": "img:test"})
    assert op.task_id == "cleanup_work"
    assert op.name == "nextflow-quality-control-cleanup"
    assert op.namespace == "nextflow"
    env = {e.name: e.value for e in op.env_vars}
    assert env["RUN_TAG"] == "qc-manual__2026-08-20T14-30-00-00-00"
    assert env["NXF_WORKSPACE"] == "/workspace"
    # Not deferrable: it is a few seconds of rm, not a multi-hour wait.
    assert op.deferrable is False
    # Mounts the PVC so it can delete scratch, and nothing else.
    assert [v.name for v in op.volumes] == ["workspace"]
    assert op.volumes[0].persistent_volume_claim.claim_name == "fsx-nextflow"


def test_qc_image_falls_back_to_the_shared_var():
    """Both pipelines are baked into one image, so the shared var is the normal case."""
    op = _build({"NEXTFLOW_OPERATOR_IMAGE": "shared:v1"})
    assert op.image == "shared:v1"
    assert _cleanup({"NEXTFLOW_OPERATOR_IMAGE": "shared:v1"}).image == "shared:v1"


def test_qc_image_can_be_pinned_without_moving_postprocessing():
    """NEXTFLOW_QC_OPERATOR_IMAGE exists to point QC at an ad-hoc build while a
    pipeline revision is being tested. Post-processing must not follow it."""
    from radiant.dags.operators.k8s import NextflowPostprocessing

    env = {
        "NEXTFLOW_OPERATOR_IMAGE": "shared:v1",
        "NEXTFLOW_QC_OPERATOR_IMAGE": "poc-nextflow:qc-2.0.0",
    }
    assert _build(env).image == "poc-nextflow:qc-2.0.0"
    # The cleanup pod must follow the driver, or it lands on a node that may not
    # have the other image cached.
    assert _cleanup(env).image == "poc-nextflow:qc-2.0.0"

    with patch.dict(os.environ, env, clear=True):
        assert (
            NextflowPostprocessing.get_run_postprocessing(input_csv="/i.csv", outdir="", run_tag="t").image
            == "shared:v1"
        )


def test_qc_image_is_none_when_nothing_is_set():
    """A missing image must fail loudly at pod creation rather than silently pull."""
    assert _build({}).image is None


def test_driver_runs_the_pipeline_from_the_shared_filesystem():
    """This pipeline ships nf-core module resource scripts (multiqc_report.py). Nextflow
    puts them on PATH by exporting the *projectDir* path into the task wrapper, but the
    task runs in the module's own container, which mounts only the workspace and has no
    /opt/nextflow. Running from the image path makes MULTIQC_PYTHON die with
    `multiqc_report.py: command not found`."""
    (script,) = _build({}).arguments
    # Staged under the workspace, keyed by the asset's commit so a rebuilt image with a
    # new revision cannot silently reuse a stale copy.
    assert 'PROJECT="${NXF_WORKSPACE}/pipelines/quality-control-pipeline-${REV}"' in script
    assert 'REV="$(git -C "$SRC" rev-parse --short HEAD)"' in script
    # And it is the staged copy that runs, never the image path.
    assert 'nextflow run "$PROJECT"' in script
    assert 'nextflow run "${NXF_HOME}' not in script
    # Concurrent drivers on the same revision must not corrupt each other's copy.
    assert 'mv -T "$TMP" "$PROJECT"' in script
