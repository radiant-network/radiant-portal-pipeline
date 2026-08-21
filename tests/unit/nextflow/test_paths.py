import pytest

from radiant.tasks.nextflow.paths import run_paths, sanitize_run_tag, split_s3_uri, to_mount

INPUTS_ROOT = "s3://qlin-nextflow-inputs"
OUTPUTS_ROOT = "s3://qlin-nextflow-outputs"


def test_run_tag_survives_a_manual_run_id():
    """Airflow's manual run ids carry a colon-and-plus timestamp, neither of which belongs
    in a filesystem path."""
    assert sanitize_run_tag("manual__2026-08-21T12:00:00+00:00") == "manual__2026-08-21T12-00-00-00-00"


def test_split_s3_uri():
    assert split_s3_uri("s3://bucket/a/b/") == ("bucket", "a/b")
    assert split_s3_uri("s3://bucket") == ("bucket", "")
    with pytest.raises(ValueError, match="not an s3 uri"):
        split_s3_uri("/workspace/inputs")


def test_to_mount_maps_the_bucket_root_to_the_mount_root():
    url = f"{INPUTS_ROOT}/individuals/NA12878/NA12878.gvcf.gz"
    assert to_mount(url, INPUTS_ROOT, "/workspace/inputs") == "/workspace/inputs/individuals/NA12878/NA12878.gvcf.gz"


def test_to_mount_rejects_another_bucket():
    with pytest.raises(ValueError, match="not in the workspace bucket"):
        to_mount("s3://elsewhere/x.gvcf.gz", INPUTS_ROOT, "/workspace/inputs")


def test_every_path_is_derived_from_the_run_tag():
    """A fresh prefix per run is structural, not a rule someone has to remember -- and each
    run needing its own output location is what lets re-runs sit alongside earlier ones."""
    paths = run_paths(INPUTS_ROOT, OUTPUTS_ROOT, "/workspace/inputs", "/workspace/outputs", "run-1")
    assert paths == {
        "run_tag": "run-1",
        "input_prefix_s3": "s3://qlin-nextflow-inputs/run-1",
        "input_prefix_pod": "/workspace/inputs/run-1",
        "samplesheet_s3": "s3://qlin-nextflow-inputs/run-1/samplesheet.csv",
        "samplesheet_pod": "/workspace/inputs/run-1/samplesheet.csv",
        "outdir_s3": "s3://qlin-nextflow-outputs/run-1",
        "outdir_pod": "/workspace/outputs/run-1",
    }


def test_a_prefix_inside_the_bucket_appears_on_both_sides():
    """The mount is the bucket root, so a root with a prefix has to keep that prefix in the
    pod path too or the two views stop pointing at the same bytes."""
    paths = run_paths(
        f"{INPUTS_ROOT}/qlin", f"{OUTPUTS_ROOT}/qlin", "/workspace/inputs", "/workspace/outputs", "run-1"
    )
    assert paths["input_prefix_s3"] == "s3://qlin-nextflow-inputs/qlin/run-1"
    assert paths["input_prefix_pod"] == "/workspace/inputs/qlin/run-1"
    assert paths["outdir_pod"] == "/workspace/outputs/qlin/run-1"
