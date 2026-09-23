import pytest

from radiant.tasks.nextflow.cnv.outputs import MissingOutputsError, collect, expected_keys
from radiant.tasks.nextflow.cnv.resolve import resolve_families

OUTDIR = "s3://qlin-nextflow-outputs/cnv/scheduled-2026-09-18T00-00-00-00-00"


@pytest.fixture
def trio(trio_rows, phenotype_rows):
    return resolve_families(trio_rows, phenotype_rows)[0]


@pytest.fixture
def listing(trio):
    return dict.fromkeys(expected_keys(trio.family_id).values(), 4096)


def test_outputs_are_keyed_on_the_family_id_alone(trio):
    """Every case takes the family route, so the pipeline names everything after the
    familyId -- never `<familyId>.<sample>` as its solo route does."""
    keys = expected_keys(trio.family_id)
    assert keys["slivar_vcf"] == "slivar/CA1072.cnv.slivar.vcf.gz"
    assert keys["exomiser_tsv"] == "exomiser/CA1072.exomiser.variants.tsv"
    assert keys["exomiser_html"] == "exomiser/CA1072.exomiser.html"
    assert keys["exomiser_json"] == "exomiser/CA1072.exomiser.json"


def test_the_annotated_vcf_is_a_germline_cnv_vcf(trio, listing):
    """`gcnv`/`vcf`: the CNV analogue of the annotation DAG's `snv`/`vcf`, and deliberately
    invisible to the variant ETL until the staging view admits it on this task type."""
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["slivar_vcf"]["data_type_code"] == "gcnv"
    assert documents["slivar_vcf"]["format_code"] == "vcf"


def test_no_index_is_expected_for_the_slivar_vcf(trio):
    """At the pinned revision `SLIVAR_EXPR` publishes `*.vcf.gz` only. Expecting a `.tbi`
    would fail every run in `collect_outputs` after the pipeline had finished, register
    nothing, and leave the cases eligible for the next night."""
    assert not [k for k in expected_keys(trio.family_id).values() if k.endswith(".tbi")]


def test_exomiser_documents_carry_the_exomiser_data_type(trio, listing):
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert {documents[n]["data_type_code"] for n in ("exomiser_tsv", "exomiser_html", "exomiser_json")} == {"exomiser"}


def test_sizes_and_names_come_from_the_listing(trio, listing):
    listing["slivar/CA1072.cnv.slivar.vcf.gz"] = 123456
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["slivar_vcf"]["size"] == 123456
    assert documents["slivar_vcf"]["name"] == "CA1072.cnv.slivar.vcf.gz"
    assert documents["slivar_vcf"]["url"] == f"{OUTDIR}/slivar/CA1072.cnv.slivar.vcf.gz"


def test_a_family_missing_one_file_fails_the_whole_collection(trio, listing):
    del listing["exomiser/CA1072.exomiser.html"]
    with pytest.raises(MissingOutputsError, match="CA1072.exomiser.html"):
        collect([trio], listing, OUTDIR)


def test_every_missing_file_is_named_at_once(trio, listing):
    del listing["exomiser/CA1072.exomiser.html"]
    del listing["slivar/CA1072.cnv.slivar.vcf.gz"]
    with pytest.raises(MissingOutputsError) as excinfo:
        collect([trio], listing, OUTDIR)
    assert "CA1072.exomiser.html" in str(excinfo.value)
    assert "CA1072.cnv.slivar.vcf.gz" in str(excinfo.value)
