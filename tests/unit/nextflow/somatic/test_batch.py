from radiant.tasks.nextflow.batch import ANNOTATION_PIPELINE
from radiant.tasks.nextflow.somatic.batch import build_patch_body
from radiant.tasks.nextflow.somatic.model import SomaticCase
from radiant.tasks.nextflow.somatic.outputs import collect
from tests.unit.nextflow.somatic.conftest import OUTDIR, calling_task


def _collected(*tasks):
    listing = {
        key: 10
        for t in tasks
        for key in (
            f"ensemblvep/variants.{t.task_id}.snv.vep.vcf.gz",
            f"ensemblvep/variants.{t.task_id}.snv.vep.vcf.gz.tbi",
        )
    }
    return collect(list(tasks), listing, OUTDIR)


def test_one_annotation_task_with_the_raw_vcf_in_and_the_vep_vcf_out():
    task = calling_task(70)
    body = build_patch_body([task], _collected(task), "radiant")

    [case] = body["cases"]
    assert (case["project_code"], case["submitter_case_id"]) == ("N1", "CASE-23")
    [annotation] = case["tasks"]
    assert annotation["type_code"] == "radiant_somatic_annotation"
    assert annotation["input_documents"] == [{"url": task.vcf_url}]
    assert [(d["name"], d["data_type_code"], d["format_code"]) for d in annotation["output_documents"]] == [
        ("variants.70.snv.vep.vcf.gz", "ssnv", "vcf"),
        ("variants.70.snv.vep.vcf.gz.tbi", "ssnv", "tbi"),
    ]
    # Same pipeline revision as germline: it pins the VEP version.
    assert (annotation["pipeline_name"], annotation["pipeline_version"]) == ANNOTATION_PIPELINE
    assert annotation["genome_build"] == "GRch38"


def test_the_aliquots_are_the_calling_task_s_tumor_first():
    tn, to = calling_task(75, "NORMAL-1"), calling_task(70)
    body = build_patch_body([tn, to], _collected(tn, to), "radiant")
    assert [t["aliquots"] for t in body["cases"][0]["tasks"]] == [["TUMOR-1", "NORMAL-1"], ["TUMOR-1"]]


def test_an_alignment_call_registers_a_single_aliquot():
    task = calling_task(76).model_copy(update={"task_type": "alignment_somatic_variant_calling"})
    body = build_patch_body([task], _collected(task), "radiant")
    assert body["cases"][0]["tasks"][0]["aliquots"] == ["TUMOR-1"]


def test_only_the_tenant_s_cases_are_in_its_batch():
    cases = [
        SomaticCase(case_id=23, submitter_case_id="CASE-23", tenant_code="radiant", project_code="N1"),
        SomaticCase(case_id=24, submitter_case_id="CASE-24", tenant_code="other", project_code="P2"),
    ]
    task = calling_task(70, cases=cases)
    assert [c["submitter_case_id"] for c in build_patch_body([task], _collected(task), "radiant")["cases"]] == [
        "CASE-23"
    ]
    assert [c["submitter_case_id"] for c in build_patch_body([task], _collected(task), "other")["cases"]] == [
        "CASE-24"
    ]
    assert build_patch_body([task], _collected(task), "none") == {"cases": []}
