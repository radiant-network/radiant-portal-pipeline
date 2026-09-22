import pytest

from radiant.tasks.nextflow.cnv.batch import build_patch_body
from radiant.tasks.nextflow.cnv.outputs import collect, expected_keys
from radiant.tasks.nextflow.cnv.resolve import resolve_families

OUTDIR = "s3://qlin-nextflow-outputs/cnv/scheduled-2026-09-18T00-00-00-00-00"


def _body(rows, phenotype_rows=()):
    families = resolve_families(rows, list(phenotype_rows))
    listing = {key: 4096 for f in families for key in expected_keys(f.family_id).values()}
    return build_patch_body(families, collect(families, listing, OUTDIR))


@pytest.fixture
def tasks(trio_rows, phenotype_rows):
    body = _body(trio_rows, phenotype_rows)
    return {t["type_code"]: t for t in body["cases"][0]["tasks"]}


def test_a_case_is_addressed_by_project_and_submitter_id(trio_rows, phenotype_rows):
    case = _body(trio_rows, phenotype_rows)["cases"][0]
    assert case["project_code"] == "N1"
    assert case["submitter_case_id"] == "1KGP-1463"


def test_two_tasks_with_the_cnv_specific_type_codes(tasks):
    """`exomiser_cnv`, not `exomiser`: the ETL ingests every `variants.tsv` of an `exomiser`
    task into the SNV Exomiser table, and a CNV report there would be wrong data."""
    assert set(tasks) == {"radiant_germline_cnv_annotation", "exomiser_cnv"}


def test_the_annotation_task_inputs_are_the_cnv_vcfs_and_the_crams_the_run_used(tasks):
    urls = [d["url"] for d in tasks["radiant_germline_cnv_annotation"]["input_documents"]]
    assert len(urls) == 3 + 3 * 2
    assert sum(u.endswith(".cnv.vcf.gz") for u in urls) == 3
    assert sum(u.endswith(".cram") for u in urls) == 3
    assert sum(u.endswith(".cram.crai") for u in urls) == 3


def test_crams_are_not_recorded_as_inputs_when_the_run_did_not_use_them(trio_rows):
    """TASK-005 aside, lineage should say what actually happened: no refinement, no CRAM."""
    trio_rows[1].update(cram_url=None, crai_url=None)
    body = _body(trio_rows)
    (annotation, _) = body["cases"][0]["tasks"]
    urls = [d["url"] for d in annotation["input_documents"]]
    assert len(urls) == 3
    assert all(u.endswith(".cnv.vcf.gz") for u in urls)


def test_exomisers_input_is_the_slivar_vcf_from_the_sibling_task(tasks):
    """Exomiser really reads the depth-refined or VEP VCF, neither of which is published as a
    document; naming one would fail TASK-005. Same compromise as the annotation DAG."""
    (input_doc,) = tasks["exomiser_cnv"]["input_documents"]
    output_urls = {d["url"] for d in tasks["radiant_germline_cnv_annotation"]["output_documents"]}
    assert input_doc["url"] in output_urls
    assert input_doc["url"].endswith(".cnv.slivar.vcf.gz")


def test_exomiser_is_single_aliquot_and_annotation_covers_the_family(tasks):
    assert tasks["exomiser_cnv"]["aliquots"] == ["NA12878"]
    assert tasks["radiant_germline_cnv_annotation"]["aliquots"] == ["NA12878", "NA12891", "NA12892"]


def test_output_documents_are_split_between_the_two_tasks(tasks):
    annotation = {
        (d["data_type_code"], d["format_code"]) for d in tasks["radiant_germline_cnv_annotation"]["output_documents"]
    }
    exomiser = {(d["data_type_code"], d["format_code"]) for d in tasks["exomiser_cnv"]["output_documents"]}
    assert annotation == {("gcnv", "vcf")}
    assert exomiser == {("exomiser", "tsv"), ("exomiser", "html"), ("exomiser", "json")}


def test_pipeline_metadata_names_the_cnv_pipeline_and_the_build(tasks):
    assert tasks["radiant_germline_cnv_annotation"]["pipeline_name"] == "cnv-post-processing"
    assert tasks["exomiser_cnv"]["pipeline_name"] == "Exomiser"
    assert all(t["genome_build"] == "GRch38" for t in tasks.values())
