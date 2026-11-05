import re
from pathlib import Path

import pytest

from radiant.tasks.nextflow.qc.batch import QC_PIPELINE, QC_TASK_TYPE, build_patch_body
from radiant.tasks.nextflow.qc.outputs import collect, expected_keys
from radiant.tasks.nextflow.qc.resolve import resolve_cases, select_cases

OUTDIR = "s3://qlin-nextflow-outputs/run"


@pytest.fixture
def cases(trio_rows, singleton_rows):
    return resolve_cases([m.model_dump() for m in select_cases(trio_rows + singleton_rows).members])


@pytest.fixture
def body(cases):
    listing = {key: 1000 for case in cases for key, _ in expected_keys(case).values()}
    return build_patch_body(cases, collect(cases, listing, OUTDIR))


def test_one_task_per_case_addressed_by_project_and_submitter_id(body):
    assert [c["submitter_case_id"] for c in body["cases"]] == ["1KGP-HG00096", "1KGP-1463"]
    assert all(c["project_code"] == "N1" and len(c["tasks"]) == 1 for c in body["cases"])


def test_the_task_binds_every_member_and_names_the_pipeline(body):
    task = body["cases"][1]["tasks"][0]
    assert task["type_code"] == QC_TASK_TYPE == "quality_control_metrics"
    assert task["aliquots"] == ["NA12878", "NA12891", "NA12892"]
    assert (task["pipeline_name"], task["pipeline_version"]) == QC_PIPELINE
    assert task["genome_build"] == "GRch38"


def test_inputs_are_the_alignment_cram_index_and_metrics_tarball(body):
    """Documents that already exist in the tenant, so TASK-005 holds however the portal
    treats TASK-003 for this task type."""
    inputs = [d["url"] for d in body["cases"][1]["tasks"][0]["input_documents"]]
    assert len(inputs) == 9
    assert all(u.endswith((".cram", ".cram.crai", ".extra.tgz")) for u in inputs)


def test_without_a_tarball_the_inputs_are_the_cram_and_index_only(body):
    """The CRAM directory the pipeline read instead is not a document."""
    inputs = [d["url"] for d in body["cases"][0]["tasks"][0]["input_documents"]]
    assert [u.rsplit("/", 1)[-1] for u in inputs] == ["HG00096.cram", "HG00096.cram.crai"]


def test_outputs_are_the_multiqc_set_and_each_member_metrics_json(body):
    outputs = body["cases"][1]["tasks"][0]["output_documents"]
    assert sorted(d["name"] for d in outputs) == [
        "CA1072_multiqc_report.html",
        "CA1072_multiqc_report_data.zip",
        "NA12878.metrics.json",
        "NA12891.metrics.json",
        "NA12892.metrics.json",
    ]
    assert {d["data_type_code"] for d in outputs} == {"aggqc"}


def test_a_singleton_registers_its_own_metrics_json(body):
    outputs = body["cases"][0]["tasks"][0]["output_documents"]
    assert [d["name"] for d in outputs if d["format_code"] == "json"] == ["HG00096.metrics.json"]


@pytest.fixture
def mixed_cases(trio_rows, singleton_rows, tumor_rows):
    return resolve_cases([m.model_dump() for m in select_cases(trio_rows + singleton_rows + tumor_rows).members])


def _collected(cases):
    listing = {key: 1000 for case in cases for key, _ in expected_keys(case).values()}
    return collect(cases, listing, OUTDIR)


def test_a_tumor_task_is_bound_to_the_tumor_with_its_tarball_alone(mixed_cases):
    """The pipeline never reads the tumor's CRAM, and the placeholder VCF of its samplesheet row is
    in no clinical document: neither is an input."""
    body = build_patch_body(mixed_cases, _collected(mixed_cases))
    tumor = next(c for c in body["cases"] if c["submitter_case_id"] == "1:23")
    task = tumor["tasks"][0]
    assert task["type_code"] == "quality_control_metrics"
    assert task["aliquots"] == ["TCRBOA6-T"]
    assert [d["url"].rsplit("/", 1)[-1] for d in task["input_documents"]] == ["TCRBOA6-T.extra.tgz"]
    assert [d["name"] for d in task["output_documents"] if d["format_code"] == "json"] == ["TCRBOA6-T.metrics.json"]


def test_a_case_without_collected_outputs_is_an_error(mixed_cases):
    """`register_tasks` passes only the cases `collect_outputs` kept; any other is a bug."""
    collected = _collected(mixed_cases)
    del collected["CA23"]
    with pytest.raises(KeyError, match="CA23"):
        build_patch_body(mixed_cases, collected)


def test_the_pipeline_version_is_the_revision_the_launcher_image_pulls():
    dockerfile = (Path(__file__).parents[4] / "Dockerfile.nextflow.launcher").read_text()
    revision = re.search(r"^ARG QC_PIPELINE_REV=(\S+)$", dockerfile, re.MULTILINE).group(1)
    assert revision.startswith(QC_PIPELINE[1])
