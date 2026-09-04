import pytest

from radiant.tasks.nextflow.outputs import MissingOutputsError, collect, expected_keys
from radiant.tasks.nextflow.resolve import resolve_families

OUTDIR = "s3://qlin-nextflow-outputs/manual__2026-08-21T12-00-00"


@pytest.fixture
def trio(trio_rows, phenotype_rows):
    return resolve_families(trio_rows, phenotype_rows)[0]


@pytest.fixture
def listing(trio):
    return dict.fromkeys(expected_keys(trio.family_id).values(), 4096)


def test_outputs_are_keyed_on_the_family_id(trio):
    """`familyId` is derived from `cases.id`, so a filename maps back to a case by
    arithmetic rather than a lookup."""
    keys = expected_keys(trio.family_id)
    assert keys["slivar_vcf"] == "slivar/variants.CA1072.snv.vep.slivar.vcf.gz"
    assert keys["slivar_tbi"] == "slivar/variants.CA1072.snv.vep.slivar.vcf.gz.tbi"
    assert keys["exomiser_tsv"] == "exomiser/CA1072.exomiser.variants.tsv"


def test_the_annotated_vcf_is_declared_vcf_not_gvcf(trio, listing):
    """`staging_external_sequencing_experiment` derives `vcf_filepath` from
    `format_code='vcf'`. Declared `gvcf`, the file would register fine and then be
    invisible to the ETL."""
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["slivar_vcf"]["format_code"] == "vcf"
    assert documents["slivar_vcf"]["data_type_code"] == "snv"


def test_the_exomiser_tsv_keeps_the_suffix_the_etl_matches_on(trio, listing):
    """`exomiser_filepath` comes from `url LIKE '%variants.tsv'`."""
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["exomiser_tsv"]["url"].endswith("variants.tsv")


def test_sizes_and_names_come_from_the_listing(trio, listing):
    """The batch API compares `size` against any existing document with the same URL and
    raises DOCUMENT-006 on a mismatch, so a size from anywhere else fails the batch."""
    listing["slivar/variants.CA1072.snv.vep.slivar.vcf.gz"] = 123456
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert documents["slivar_vcf"]["size"] == 123456
    assert documents["slivar_vcf"]["name"] == "variants.CA1072.snv.vep.slivar.vcf.gz"
    assert documents["slivar_vcf"]["url"] == f"{OUTDIR}/slivar/variants.CA1072.snv.vep.slivar.vcf.gz"


def test_a_family_missing_one_file_fails_the_whole_collection(trio, listing):
    """A partially-successful Nextflow run must not become a partially-registered case
    set, and the run's task status does not distinguish the two."""
    del listing["exomiser/CA1072.exomiser.html"]
    with pytest.raises(MissingOutputsError, match="CA1072.exomiser.html"):
        collect([trio], listing, OUTDIR)


def test_every_missing_file_is_named_at_once(trio, listing):
    del listing["exomiser/CA1072.exomiser.html"]
    del listing["slivar/variants.CA1072.snv.vep.slivar.vcf.gz.tbi"]
    with pytest.raises(MissingOutputsError) as excinfo:
        collect([trio], listing, OUTDIR)
    assert "CA1072.exomiser.html" in str(excinfo.value)
    assert "slivar.vcf.gz.tbi" in str(excinfo.value)
