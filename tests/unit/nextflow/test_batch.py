import pytest

from radiant.tasks.nextflow.batch import build_patch_body
from radiant.tasks.nextflow.outputs import collect, expected_keys
from radiant.tasks.nextflow.resolve import resolve_families

OUTDIR = "s3://qlin-nextflow-outputs/manual__2026-08-21T12-00-00"


@pytest.fixture
def body(trio_rows, phenotype_rows):
    families = resolve_families(trio_rows, phenotype_rows, [1072])
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


def test_both_task_types_carry_input_documents(tasks):
    """Both are in RequiresInputDocumentsTaskTypes; omitting them is TASK-003 and fails
    the whole batch."""
    assert len(tasks["radiant_germline_annotation"]["input_documents"]) == 3
    assert len(tasks["exomiser"]["input_documents"]) == 1


def test_the_annotation_task_inputs_are_the_member_gvcfs(tasks):
    urls = {d["url"] for d in tasks["radiant_germline_annotation"]["input_documents"]}
    assert all(url.endswith(".hard-filtered.gvcf.gz") for url in urls)
    assert len(urls) == 3


def test_exomisers_input_is_the_slivar_vcf_from_the_sibling_task(tasks):
    """The run actually feeds Exomiser the VEP-annotated VCF, but that file is published as
    no document, so naming it would fail TASK-005. Resolving the slivar VCF in-batch is the
    documented compromise."""
    (input_doc,) = tasks["exomiser"]["input_documents"]
    output_urls = {d["url"] for d in tasks["radiant_germline_annotation"]["output_documents"]}
    assert input_doc["url"] in output_urls
    assert input_doc["url"].endswith(".slivar.vcf.gz")


def test_exomiser_is_single_aliquot(tasks):
    """`exomiser` is in SingleAliquotTaskTypes; more than one aliquot is TASK-007."""
    assert tasks["exomiser"]["aliquots"] == ["NA12878"]


def test_the_annotation_task_covers_every_family_member(tasks):
    assert tasks["radiant_germline_annotation"]["aliquots"] == ["NA12878", "NA12891", "NA12892"]


def test_output_documents_are_split_between_the_two_tasks(tasks):
    annotation = {d["format_code"] for d in tasks["radiant_germline_annotation"]["output_documents"]}
    exomiser = {d["format_code"] for d in tasks["exomiser"]["output_documents"]}
    assert annotation == {"vcf", "tbi"}
    assert exomiser == {"tsv", "html", "json"}


def test_pipeline_metadata_names_the_post_processing_pipeline(tasks):
    """Not the variant caller upstream of this run."""
    assert tasks["radiant_germline_annotation"]["pipeline_name"] == "Post-processing-Pipeline"
    assert tasks["exomiser"]["pipeline_name"] == "Exomiser"
    assert all(t["genome_build"] == "GRch38" for t in tasks.values())
