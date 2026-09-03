import pytest

from radiant.tasks.nextflow.qc.metrics import (
    candidate_dirs,
    common_ancestor,
    group_by_dir,
    locate_metrics,
    metrics_filenames,
    parent_dir,
)
from radiant.tasks.nextflow.qc.resolve import resolve_cases, select_cases

from .conftest import BUCKET, INPUTS_ROOT, fake_listers

RUN42 = f"{BUCKET}/dragen/run-42"
RUN43 = f"{BUCKET}/dragen/run-43"


@pytest.fixture
def cases(trio_rows, singleton_rows):
    selection = select_cases(trio_rows + singleton_rows)
    return resolve_cases([m.model_dump() for m in selection.members])


def test_both_dragen_spellings_are_recognised():
    assert metrics_filenames("NA12878") == {"NA12878.mapping_metrics.csv", "NA12878.final.mapping_metrics.csv"}


def test_parent_dir_handles_files_and_directory_urls():
    assert parent_dir("s3://b/a/c/x.cram") == "s3://b/a/c"
    assert parent_dir("s3://b/a/c/") == "s3://b/a/c"
    assert parent_dir("s3://b/x.cram") == "s3://b"


def test_candidates_are_the_directories_of_every_alignment_output(cases):
    proband = cases[1].proband
    assert candidate_dirs(proband) == [f"{RUN42}/NA12878", f"{RUN42}/cram"]


def test_common_ancestor():
    assert common_ancestor([f"{RUN42}/a", f"{RUN42}/b/c"]) == RUN42
    assert common_ancestor(["s3://one/a", "s3://two/a"]) is None


def test_metrics_next_to_the_gvcf_are_found(cases):
    """The layout that motivated probing over convention: metrics beside the gVCF, not the CRAM."""
    files = {f"{RUN42}/{a}": {f"{a}.mapping_metrics.csv"} for a in ("NA12878", "NA12891", "NA12892")}
    kept, excluded = locate_metrics([cases[1]], *fake_listers(files), INPUTS_ROOT)
    assert excluded == []
    assert [m.metrics_dir_s3 for m in kept[0].members] == [f"{RUN42}/NA12878", f"{RUN42}/NA12891", f"{RUN42}/NA12892"]


def test_a_case_split_across_directories_uses_the_common_ancestor_when_safe(cases):
    files = {f"{RUN42}/{a}": {f"{a}.final.mapping_metrics.csv"} for a in ("NA12878", "NA12891", "NA12892")}
    kept, _ = locate_metrics([cases[1]], *fake_listers(files), INPUTS_ROOT)
    assert kept[0].metrics_dir_s3 == RUN42


def test_a_duplicated_sample_under_the_ancestor_excludes_the_case(cases):
    """The corner case: the same aliquot re-aligned under the ancestor would attach both files."""
    files = {f"{RUN42}/{a}": {f"{a}.mapping_metrics.csv"} for a in ("NA12878", "NA12891", "NA12892")}
    files[f"{RUN42}/rerun/NA12878"] = {"NA12878.mapping_metrics.csv"}
    kept, excluded = locate_metrics([cases[1]], *fake_listers(files), INPUTS_ROOT)
    assert kept == []
    assert excluded[0].reason == "metrics_dir_split"
    assert "holds 2" in excluded[0].detail


def test_metrics_in_one_directory_for_the_whole_case(cases):
    files = {f"{RUN42}/cram": {f"{a}.mapping_metrics.csv" for a in ("NA12878", "NA12891", "NA12892")}}
    kept, _ = locate_metrics([cases[1]], *fake_listers(files), INPUTS_ROOT)
    assert kept[0].metrics_dir_s3 == f"{RUN42}/cram"


def test_no_metrics_anywhere_excludes_with_a_reason(cases):
    kept, excluded = locate_metrics([cases[1]], *fake_listers({}), INPUTS_ROOT)
    assert kept == []
    assert excluded[0].reason == "no_dragen_metrics"
    assert "NA12878" in excluded[0].detail


def test_metrics_in_two_candidate_directories_are_ambiguous(cases):
    files = {
        f"{RUN42}/cram": {"NA12878.mapping_metrics.csv"},
        f"{RUN42}/NA12878": {"NA12878.mapping_metrics.csv"},
    }
    _, excluded = locate_metrics([cases[1]], *fake_listers(files), INPUTS_ROOT)
    assert excluded[0].reason == "ambiguous_dragen_metrics"


def test_metrics_outside_the_workspace_bucket_are_refused(cases):
    files = {f"{RUN42}/cram": {f"{a}.mapping_metrics.csv" for a in ("NA12878", "NA12891", "NA12892")}}
    _, excluded = locate_metrics([cases[1]], *fake_listers(files), "s3://another-bucket/inputs")
    assert excluded[0].reason == "metrics_not_on_workspace"


def test_one_group_per_directory_with_stable_run_tags(cases):
    files = {
        f"{RUN42}/cram": {f"{a}.mapping_metrics.csv" for a in ("NA12878", "NA12891", "NA12892")},
        f"{RUN43}/cram": {"HG00096.mapping_metrics.csv"},
    }
    kept, _ = locate_metrics(cases, *fake_listers(files), INPUTS_ROOT)
    groups = group_by_dir(kept, "scheduled__2026-09-02T00-00-00-00-00")
    assert [(g.run_tag, g.metrics_dir_s3, g.case_ids) for g in groups] == [
        ("scheduled__2026-09-02T00-00-00-00-00-g0", f"{RUN42}/cram", [1072]),
        ("scheduled__2026-09-02T00-00-00-00-00-g1", f"{RUN43}/cram", [8]),
    ]


def test_cases_sharing_a_directory_share_a_run(cases):
    files = {f"{RUN42}/cram": {f"{a}.mapping_metrics.csv" for a in ("NA12878", "NA12891", "NA12892", "HG00096")}}
    for case in cases:
        for member in case.members:
            member.cram_url = f"{RUN42}/cram/{member.aliquot}.cram"
            member.crai_url = None
            member.document_urls = []
    kept, _ = locate_metrics(cases, *fake_listers(files), INPUTS_ROOT)
    groups = group_by_dir(kept, "run")
    assert len(groups) == 1 and groups[0].case_ids == [8, 1072]
