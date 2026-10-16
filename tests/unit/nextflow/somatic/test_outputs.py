import pytest

from radiant.tasks.nextflow.somatic.outputs import MissingOutputsError, collect, expected_keys
from tests.unit.nextflow.somatic.conftest import OUTDIR, calling_task


def test_the_outputs_are_named_after_the_calling_task():
    assert expected_keys("70") == {
        "vep_vcf": "ensemblvep/variants.70.snv.vep.vcf.gz",
        "vep_tbi": "ensemblvep/variants.70.snv.vep.vcf.gz.tbi",
    }


def test_collect_types_the_vcf_and_its_index_as_somatic_snv():
    listing = {"ensemblvep/variants.70.snv.vep.vcf.gz": 123, "ensemblvep/variants.70.snv.vep.vcf.gz.tbi": 7}
    documents = collect([calling_task(70)], listing, OUTDIR)["70"]
    assert documents["vep_vcf"] == {
        "name": "variants.70.snv.vep.vcf.gz",
        "url": f"{OUTDIR}/ensemblvep/variants.70.snv.vep.vcf.gz",
        "size": 123,
        "data_category_code": "genomic",
        "data_type_code": "ssnv",
        "format_code": "vcf",
    }
    assert (documents["vep_tbi"]["data_type_code"], documents["vep_tbi"]["format_code"]) == ("ssnv", "tbi")
    assert documents["vep_tbi"]["size"] == 7


def test_a_missing_output_fails_the_run_naming_the_task_and_its_vcf():
    listing = {"ensemblvep/variants.70.snv.vep.vcf.gz": 123}
    with pytest.raises(MissingOutputsError) as error:
        collect([calling_task(70)], listing, OUTDIR)
    assert "calling task 70" in str(error.value)
    assert calling_task(70).vcf_url in str(error.value)
    assert "variants.70.snv.vep.vcf.gz.tbi" in str(error.value)
