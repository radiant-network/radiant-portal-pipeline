import pytest

from radiant.tasks.nextflow.qc.batch import QC_PIPELINE, QC_TASK_TYPE, build_patch_body
from radiant.tasks.nextflow.qc.outputs import collect, expected_keys
from radiant.tasks.nextflow.qc.resolve import resolve_cases, select_cases

OUTDIR = "s3://qlin-nextflow-outputs/run-g0"


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


def test_inputs_are_the_alignment_cram_and_index(body):
    """Documents that already exist in the tenant, so TASK-005 holds however the portal
    treats TASK-003 for this task type."""
    inputs = [d["url"] for d in body["cases"][1]["tasks"][0]["input_documents"]]
    assert len(inputs) == 6
    assert all(u.endswith((".cram", ".cram.crai")) for u in inputs)


def test_outputs_are_the_whole_multiqc_set(body):
    outputs = body["cases"][1]["tasks"][0]["output_documents"]
    assert sorted(d["name"] for d in outputs) == [
        "CA1072_multiqc_report.html",
        "CA1072_multiqc_report_data.zip",
        "NA12878.metrics.json",
        "NA12891.metrics.json",
        "NA12892.metrics.json",
    ]
    assert {d["data_type_code"] for d in outputs} == {"aggqc"}
