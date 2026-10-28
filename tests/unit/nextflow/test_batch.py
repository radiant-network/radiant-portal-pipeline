import pytest

from radiant.tasks.nextflow.batch import build_patch_body
from radiant.tasks.nextflow.outputs import collect, expected_keys
from radiant.tasks.nextflow.resolve import resolve_families

OUTDIR = "s3://qlin-nextflow-outputs/manual__2026-08-21T12-00-00"


@pytest.fixture
def body(trio_rows, phenotype_rows):
    families = resolve_families(trio_rows, phenotype_rows)
    listing = dict.fromkeys(expected_keys("CA1072").values(), 4096)
    return build_patch_body(families, collect(families, listing, OUTDIR))


@pytest.fixture
def tasks(body):
    return {t["type_code"]: t for t in body["cases"][0]["tasks"]}


def test_a_case_is_addressed_by_project_and_submitter_id(body):
    """That pair, not `cases.id`, is what the batch PATCH looks a case up by -- while the
    output files are keyed on `CA<case id>`. The two keys are not interchangeable."""
    case = body["cases"][0]
    assert case["project_code"] == "N1"
    assert case["submitter_case_id"] == "1KGP-1463"


def test_one_annotation_task_per_case_and_no_legacy_exomiser_task(body, tasks):
    """The Exomiser reports are outputs of the annotation task, so each run of a
    sequencing keeps its own Exomiser results. The `exomiser` task type is gone."""
    assert len(body["cases"][0]["tasks"]) == 1
    assert set(tasks) == {"radiant_germline_annotation"}


def test_the_annotation_task_carries_input_documents(tasks):
    """It is in RequiresInputDocumentsTaskTypes; omitting them is TASK-003 and fails the
    whole batch."""
    assert len(tasks["radiant_germline_annotation"]["input_documents"]) == 3


def test_the_annotation_task_inputs_are_the_member_gvcfs(tasks):
    urls = {d["url"] for d in tasks["radiant_germline_annotation"]["input_documents"]}
    assert all(url.endswith(".hard-filtered.gvcf.gz") for url in urls)
    assert len(urls) == 3


def test_the_annotation_task_covers_every_family_member(tasks):
    assert tasks["radiant_germline_annotation"]["aliquots"] == ["NA12878", "NA12891", "NA12892"]


def test_the_annotation_task_outputs_the_slivar_vcf_and_the_exomiser_reports(tasks):
    """The staging view reads the Exomiser tsv from the annotation task only with the
    `exomiser` data type: the SNV reports keep it."""
    outputs = {
        (d["data_type_code"], d["format_code"]) for d in tasks["radiant_germline_annotation"]["output_documents"]
    }
    assert outputs == {("snv", "vcf"), ("snv", "tbi"), ("exomiser", "tsv"), ("exomiser", "html"), ("exomiser", "json")}


def test_pipeline_metadata_names_the_post_processing_pipeline(tasks):
    """Not the variant caller upstream of this run."""
    assert tasks["radiant_germline_annotation"]["pipeline_name"] == "snv-post-processing"
    assert all(t["genome_build"] == "GRch38" for t in tasks.values())
