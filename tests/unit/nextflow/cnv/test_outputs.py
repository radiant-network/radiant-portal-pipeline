import pytest

from radiant.tasks.nextflow.cnv.outputs import MissingOutputsError, collect, expected_keys
from radiant.tasks.nextflow.cnv.resolve import resolve_families

OUTDIR = "s3://qlin-nextflow-outputs/cnv/scheduled-2026-09-18T00-00-00-00-00"


@pytest.fixture
def trio(trio_rows, phenotype_rows):
    return resolve_families(trio_rows, phenotype_rows)[0]


@pytest.fixture
def listing(trio):
    """What a run publishes for a family whose Exomiser ran on the proband VCF: the preferred name of each output."""
    return {candidates[0]: 4096 for candidates in expected_keys(trio.family_id, trio.proband.aliquot).values()}


@pytest.fixture
def family_vcf_listing(trio):
    """What a run publishes for a family whose Exomiser ran on the family VCF: reports named after the family alone."""
    return {candidates[-1]: 4096 for candidates in expected_keys(trio.family_id, trio.proband.aliquot).values()}


def test_the_slivar_vcf_is_keyed_on_the_family_id_and_the_exomiser_reports_on_the_proband_too():
    """Every case takes the family route, so the slivar VCF is named after the familyId alone. Exomiser
    runs on the proband VCF and names its reports `<familyId>.<proband>`, after the family VCF's
    `<familyId>` when the family has no proband VCF: both are accepted, the proband one first."""
    keys = expected_keys("CA1072", "NA12878")
    assert keys["slivar_vcf"] == ["slivar/CA1072.cnv.slivar.vcf.gz"]
    assert keys["slivar_tbi"] == ["slivar/CA1072.cnv.slivar.vcf.gz.tbi"]
    assert keys["exomiser_tsv"] == [
        "exomiser/CA1072.NA12878.exomiser.variants.tsv",
        "exomiser/CA1072.exomiser.variants.tsv",
    ]
    assert keys["exomiser_html"] == ["exomiser/CA1072.NA12878.exomiser.html", "exomiser/CA1072.exomiser.html"]
    assert keys["exomiser_json"] == ["exomiser/CA1072.NA12878.exomiser.json", "exomiser/CA1072.exomiser.json"]


def test_the_proband_is_the_phenopacket_subject(trio):
    """The pipeline names the reports after the phenopacket's `proband.subject.id`, which `build_phenopacket`
    writes as the proband's aliquot: the same member, the same string."""
    assert trio.proband.aliquot == "NA12878"
    assert expected_keys(trio.family_id, trio.proband.aliquot)["exomiser_html"][0] == (
        "exomiser/CA1072.NA12878.exomiser.html"
    )


def test_the_annotated_vcf_is_a_germline_cnv_vcf(trio, listing):
    """`gcnv`/`vcf`: the CNV analogue of the annotation DAG's `snv`/`vcf`, and deliberately
    invisible to the variant ETL until the staging view admits it on this task type."""
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["slivar_vcf"]["data_type_code"] == "gcnv"
    assert documents["slivar_vcf"]["format_code"] == "vcf"


def test_the_slivar_index_is_a_germline_cnv_tbi(trio, listing):
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["slivar_tbi"]["data_type_code"] == "gcnv"
    assert documents["slivar_tbi"]["format_code"] == "tbi"


def test_exomiser_documents_carry_the_exomiser_cnv_data_type(trio, listing):
    """`exomiser_cnv`, not `exomiser`: the portal tells the CNV report from the SNV one, and the
    staging view reads only `exomiser` into the SNV Exomiser table."""
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert {documents[n]["data_type_code"] for n in ("exomiser_tsv", "exomiser_html", "exomiser_json")} == {
        "exomiser_cnv"
    }


def test_sizes_and_names_come_from_the_listing(trio, listing):
    listing["slivar/CA1072.cnv.slivar.vcf.gz"] = 123456
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["slivar_vcf"]["size"] == 123456
    assert documents["slivar_vcf"]["name"] == "CA1072.cnv.slivar.vcf.gz"
    assert documents["slivar_vcf"]["url"] == f"{OUTDIR}/slivar/CA1072.cnv.slivar.vcf.gz"
    assert documents["exomiser_html"]["name"] == "CA1072.NA12878.exomiser.html"
    assert documents["exomiser_html"]["url"] == f"{OUTDIR}/exomiser/CA1072.NA12878.exomiser.html"


def test_a_family_run_on_its_family_vcf_registers_the_family_named_reports(trio, family_vcf_listing):
    """A family without a proband VCF gets `<familyId>.exomiser.*`; it is a complete set all the same."""
    documents = collect([trio], family_vcf_listing, OUTDIR)["CA1072"]
    assert documents["exomiser_html"]["name"] == "CA1072.exomiser.html"
    assert documents["exomiser_html"]["url"] == f"{OUTDIR}/exomiser/CA1072.exomiser.html"
    assert documents["exomiser_html"]["data_type_code"] == "exomiser_cnv"


def test_the_proband_named_report_wins_when_both_are_listed(trio, listing, family_vcf_listing):
    """One family-mode run per family, so this never happens in one run; the choice is deterministic anyway."""
    documents = collect([trio], listing | family_vcf_listing, OUTDIR)["CA1072"]
    assert documents["exomiser_json"]["name"] == "CA1072.NA12878.exomiser.json"


def test_a_family_missing_one_file_fails_the_whole_collection(trio, listing):
    """The error names both names the report may have had, so the operator finds it under either."""
    del listing["exomiser/CA1072.NA12878.exomiser.html"]
    with pytest.raises(MissingOutputsError) as excinfo:
        collect([trio], listing, OUTDIR)
    assert "exomiser/CA1072.NA12878.exomiser.html or exomiser/CA1072.exomiser.html" in str(excinfo.value)


def test_every_missing_file_is_named_at_once(trio, listing):
    del listing["exomiser/CA1072.NA12878.exomiser.html"]
    del listing["slivar/CA1072.cnv.slivar.vcf.gz"]
    with pytest.raises(MissingOutputsError) as excinfo:
        collect([trio], listing, OUTDIR)
    assert "CA1072.NA12878.exomiser.html" in str(excinfo.value)
    assert "CA1072.cnv.slivar.vcf.gz" in str(excinfo.value)
