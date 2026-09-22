import os
from unittest.mock import patch

from radiant.dags.operators.k8s import NextflowCnvPostprocessing

RUN_TAG = "cnv-scheduled-2026-09-18T00-00-00-00-00"


def _build(env: dict):
    with patch.dict(os.environ, env, clear=True):
        return NextflowCnvPostprocessing.get_run_cnv_postprocessing(
            input_csv="/workspace/inputs/cnv-runs/run1/samplesheet.csv",
            outdir="/workspace/outputs/cnv/run1",
            run_tag=RUN_TAG,
        )


def _cleanup(env: dict):
    with patch.dict(os.environ, env, clear=True):
        return NextflowCnvPostprocessing.get_cleanup_work(run_tag=RUN_TAG)


def test_get_run_cnv_postprocessing_with_env():
    op = _build(
        {
            "NEXTFLOW_OPERATOR_IMAGE": "my-registry/nextflow-launcher:test",
            "NEXTFLOW_OPERATOR_KUBERNETES_NAMESPACE": "nf",
            "NEXTFLOW_OPERATOR_SERVICE_ACCOUNT_NAME": "nf-sa",
            "NEXTFLOW_OPERATOR_PVC_NAME": "fsx-test",
            "NEXTFLOW_OPERATOR_NODEPOOL": "test-pool",
            "NEXTFLOW_OPERATOR_WORKSPACE_PATH": "/ws",
        }
    )
    assert op.task_id == "run_cnv_postprocessing"
    assert op.name == "nextflow-cnv-postprocessing-driver"
    assert op.namespace == "nf"
    assert op.service_account_name == "nf-sa"
    assert op.image == "my-registry/nextflow-launcher:test"
    assert op.deferrable is True
    assert op.node_selector == {"nodepool": "test-pool"}
    assert op.annotations == {"karpenter.sh/do-not-disrupt": "true"}

    volumes = {v.name: v for v in op.volumes}
    assert set(volumes) == {"workspace", "nextflow-cfg", "nextflow-params"}
    assert volumes["workspace"].persistent_volume_claim.claim_name == "fsx-test"
    mounts = {m.name: m for m in op.volume_mounts}
    assert mounts["workspace"].mount_path == "/ws"
    assert mounts["nextflow-cfg"].mount_path == "/etc/nextflow"
    assert mounts["nextflow-params"].mount_path == "/etc/nextflow-params"

    env = {e.name: e.value for e in op.env_vars}
    assert env["NXF_INPUT"] == "/workspace/inputs/cnv-runs/run1/samplesheet.csv"
    assert env["NXF_OUTDIR"] == "/workspace/outputs/cnv/run1"
    assert env["RUN_TAG"] == RUN_TAG
    assert env["NXF_WORKSPACE"] == "/ws"
    # Pod Identity provides the credentials; empty AWS_* would break the chain.
    assert not [name for name in env if name.startswith("AWS_")]


def test_cnv_uses_its_own_configmaps():
    """A `-c` config referencing a param the `-params-file` does not declare kills the run at
    parse, so each pipeline has its own pair."""
    volumes = {v.name: v for v in _build({}).volumes}
    assert volumes["nextflow-cfg"].config_map.name == "nextflow-cnv-cfg"
    assert volumes["nextflow-params"].config_map.name == "nextflow-cnv-params"

    overridden = _build(
        {
            "NEXTFLOW_CNV_OPERATOR_CONFIG_CONFIGMAP": "cnv-cfg-test",
            "NEXTFLOW_CNV_OPERATOR_PARAMS_CONFIGMAP": "cnv-params-test",
        }
    )
    volumes = {v.name: v for v in overridden.volumes}
    assert volumes["nextflow-cfg"].config_map.name == "cnv-cfg-test"
    assert volumes["nextflow-params"].config_map.name == "cnv-params-test"


def test_defaults_match_the_qa_topology():
    op = _build({})
    assert op.namespace == "nextflow"
    assert op.service_account_name == "nextflow"
    assert op.node_selector == {"nodepool": "qlin-nextflow"}
    assert op.volumes[0].persistent_volume_claim.claim_name == "fsx-nextflow"
    assert op.startup_timeout_seconds == 1800
    assert op.active_deadline_seconds == 86400
    # A missing image must fail loudly at pod creation rather than silently pull.
    assert op.image is None


def test_driver_script_is_jinja_safe_and_runs_the_cnv_pipeline():
    (script,) = _build({}).arguments
    assert "{{" not in script
    assert "{%" not in script
    assert 'cd "$LAUNCH"' in script
    assert "-resume" in script
    assert "assets/Ferlab-Ste-Justine/cnv-post-processing" in script
    assert "Post-processing-Pipeline" not in script
    assert "quality-control-pipeline" not in script
    # Nothing beyond the samplesheet and outdir on the command line.
    assert "--dragen_metrics_dir" not in script
    assert '--input "$NXF_INPUT"' in script
    assert '--outdir "$OUTDIR"' in script


def test_driver_runs_the_pipeline_from_the_shared_filesystem():
    """This pipeline ships `bin/refine_genotypes.py`, called by name from the
    DEPTH_GENOTYPE_REFINE process. Nextflow exports the *projectDir* `bin/` onto the task's
    PATH, but the task runs in the pysam container, which mounts only the workspace -- run
    from the image path the process dies with `command not found` (exit 127), exactly as
    the QC pipeline's MultiQC script did."""
    (script,) = _build({}).arguments
    assert 'PROJECT="${NXF_WORKSPACE}/pipelines/cnv-post-processing-${REV}"' in script
    assert 'REV="$(git -C "$SRC" rev-parse --short HEAD)"' in script
    assert 'nextflow run "$PROJECT"' in script
    assert 'nextflow run "${NXF_HOME}' not in script
    assert 'mv -T "$TMP" "$PROJECT"' in script


def test_cleanup_targets_only_this_run():
    op = _cleanup({"NEXTFLOW_OPERATOR_IMAGE": "img:test"})
    assert op.task_id == "cleanup_work"
    assert op.name == "nextflow-cnv-postprocessing-cleanup"
    env = {e.name: e.value for e in op.env_vars}
    assert env["RUN_TAG"] == RUN_TAG
    assert op.deferrable is False
    assert [v.name for v in op.volumes] == ["workspace"]
    (script,) = op.arguments
    assert "${RUN_TAG:?" in script


def test_cnv_image_falls_back_to_the_shared_var_and_can_be_pinned_alone():
    from radiant.dags.operators.k8s import NextflowPostprocessing, NextflowQualityControl

    assert _build({"NEXTFLOW_OPERATOR_IMAGE": "shared:v1"}).image == "shared:v1"
    assert _cleanup({"NEXTFLOW_OPERATOR_IMAGE": "shared:v1"}).image == "shared:v1"

    env = {"NEXTFLOW_OPERATOR_IMAGE": "shared:v1", "NEXTFLOW_CNV_OPERATOR_IMAGE": "poc:cnv"}
    assert _build(env).image == "poc:cnv"
    assert _cleanup(env).image == "poc:cnv"
    with patch.dict(os.environ, env, clear=True):
        assert NextflowPostprocessing.get_run_postprocessing(input_csv="/i.csv", outdir="", run_tag="t").image == (
            "shared:v1"
        )
        assert (
            NextflowQualityControl.get_run_quality_control(
                input_csv="/i.csv", outdir="", dragen_metrics_dir="/m", run_tag="t"
            ).image
            == "shared:v1"
        )
