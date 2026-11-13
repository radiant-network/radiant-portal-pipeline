import pytest

from radiant.tasks.nextflow.germline_cnv.batch import build_patch_body
from radiant.tasks.nextflow.germline_cnv.outputs import collect, expected_keys
from radiant.tasks.nextflow.germline_cnv.resolve import resolve_families

OUTDIR = "s3://qlin-nextflow-outputs/cnv/scheduled-2026-09-18T00-00-00-00-00"


def _body(rows, phenotype_rows=()):
    families = resolve_families(rows, list(phenotype_rows))
    listing = {
        candidates[0]: 4096 for f in families for candidates in expected_keys(f.family_id, f.proband.aliquot).values()
    }
    return build_patch_body(families, collect(families, listing, OUTDIR))


@pytest.fixture
def tasks(trio_rows, phenotype_rows):
    body = _body(trio_rows, phenotype_rows)
    return {t["type_code"]: t for t in body["cases"][0]["tasks"]}


def test_a_case_is_addressed_by_project_and_submitter_id(trio_rows, phenotype_rows):
    case = _body(trio_rows, phenotype_rows)["cases"][0]
    assert case["project_code"] == "N1"
    assert case["submitter_case_id"] == "1KGP-1463"


def test_one_annotation_task_per_case_and_no_legacy_exomiser_cnv_task(trio_rows, phenotype_rows, tasks):
    """The Exomiser reports are outputs of the annotation task. The `exomiser_cnv`
    task type is gone."""
    assert len(_body(trio_rows, phenotype_rows)["cases"][0]["tasks"]) == 1
    assert set(tasks) == {"radiant_germline_cnv_annotation"}


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
    (annotation,) = body["cases"][0]["tasks"]
    urls = [d["url"] for d in annotation["input_documents"]]
    assert len(urls) == 3
    assert all(u.endswith(".cnv.vcf.gz") for u in urls)


def test_the_annotation_task_covers_the_family(tasks):
    assert tasks["radiant_germline_cnv_annotation"]["aliquots"] == ["NA12878", "NA12891", "NA12892"]


def test_the_annotation_task_outputs_the_slivar_vcf_and_the_exomiser_cnv_reports(tasks):
    """`exomiser_cnv`, not `exomiser`: the portal tells the CNV report from the SNV one, and the
    staging view reads only `exomiser` into the SNV Exomiser table."""
    outputs = {
        (d["data_type_code"], d["format_code"]) for d in tasks["radiant_germline_cnv_annotation"]["output_documents"]
    }
    assert outputs == {
        ("gcnv", "vcf"),
        ("gcnv", "tbi"),
        ("exomiser_cnv", "tsv"),
        ("exomiser_cnv", "html"),
        ("exomiser_cnv", "json"),
    }


def test_the_exomiser_reports_are_registered_under_the_name_the_run_gave_them(tasks):
    """Named after the family and its proband when Exomiser ran on the proband VCF: the document name
    follows the listing, so the portal shows the file as it sits on S3."""
    names = {d["name"] for d in tasks["radiant_germline_cnv_annotation"]["output_documents"]}
    expected = {f"CA1072.NA12878.exomiser.{ext}" for ext in ("variants.tsv", "html", "json")}
    assert expected <= names


def test_pipeline_metadata_names_the_cnv_pipeline_and_the_build(tasks):
    assert tasks["radiant_germline_cnv_annotation"]["pipeline_name"] == "cnv-post-processing"
    assert all(t["genome_build"] == "GRch38" for t in tasks.values())
