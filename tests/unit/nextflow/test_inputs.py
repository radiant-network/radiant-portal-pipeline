import pytest

from radiant.tasks.nextflow.inputs import build_inputs, build_ped, build_phenopacket, build_samplesheet, yaml_str
from radiant.tasks.nextflow.resolve import resolve_families

INPUTS_ROOT = "s3://qlin-nextflow-inputs"
INPUTS_MOUNT = "/workspace/inputs"
PREFIX_POD = "/workspace/inputs/manual__2026-08-21T12-00-00"


@pytest.fixture
def trio(trio_rows, phenotype_rows):
    return resolve_families(trio_rows, phenotype_rows, [1072])[0]


@pytest.fixture
def singleton(singleton_rows):
    return resolve_families(singleton_rows, [], [8])[0]


def test_samplesheet_columns_are_the_ones_nf_schema_expects(trio):
    csv = build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    assert csv.splitlines()[0] == "familyId,sample,sequencingType,gvcf,familyPheno,familyPed"


def test_samplesheet_references_pod_paths_not_s3_uris(trio):
    """The pipeline reads these off the FSx mount. An s3:// uri in the CSV would only fail
    once a worker tried to open it, hours in."""
    csv = build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    assert "s3://" not in csv
    row = csv.splitlines()[1].split(",")
    assert row[0] == "CA1072"
    assert row[2] == "WGS"
    assert row[3] == f"{INPUTS_MOUNT}/individuals/NA12878/NA12878.hard-filtered.gvcf.gz"
    assert row[4] == f"{PREFIX_POD}/phenotypes/CA1072.yml"
    assert row[5] == f"{PREFIX_POD}/pedigrees/CA1072.ped"


def test_samplesheet_has_one_row_per_member(trio, singleton):
    csv = build_samplesheet([trio, singleton], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    assert len(csv.splitlines()) == 1 + 3 + 1


def test_a_gvcf_outside_the_workspace_bucket_is_rejected(trio):
    """It would not be on the FSx mount at all, and the run would die in a worker pod
    instead of here."""
    trio.members[0].gvcf_url = "s3://some-other-bucket/x.gvcf.gz"
    with pytest.raises(ValueError, match="not in the workspace bucket"):
        build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)


def test_ped_names_the_parents_on_the_proband_row_only(trio):
    assert build_ped(trio).splitlines() == [
        "CA1072\tNA12878\tNA12891\tNA12892\t2\t2",
        "CA1072\tNA12891\t0\t0\t1\t2",
        "CA1072\tNA12892\t0\t0\t2\t1",
    ]


def test_ped_for_a_singleton_has_no_parents(singleton):
    assert build_ped(singleton) == "CA8\tHG00096\t0\t0\t1\t2\n"


def test_phenopacket_puts_observed_terms_before_excluded_ones(trio):
    """Exomiser ranks on observed terms; excluded ones only penalise."""
    yml = build_phenopacket(trio)
    assert yml.index("HP:0001249") < yml.index("HP:0000618")
    assert "excluded: true" in yml.split("HP:0000618", 1)[1]
    assert "excluded: true" not in yml.split("HP:0001249", 1)[1].split("HP:0000618", 1)[0]


def test_phenopacket_carries_the_pedigree_and_the_proband(trio):
    yml = build_phenopacket(trio)
    assert "id: CA1072" in yml
    assert "    id: NA12878" in yml
    assert "      paternalId: NA12891" in yml
    assert "      maternalId: NA12892" in yml
    assert "affectedStatus: UNAFFECTED" in yml
    assert "phenopacketSchemaVersion: 2.0" in yml


def test_phenopacket_omits_the_features_block_when_there_are_no_terms(singleton):
    assert "phenotypicFeatures" not in build_phenopacket(singleton)


@pytest.mark.parametrize("value", ["1072", "12", "true", "no", "~"])
def test_yaml_str_quotes_values_yaml_would_retype(value):
    """Phenopacket ids are string fields; an all-digit id would load as an int."""
    assert yaml_str(value) == f'"{value}"'


@pytest.mark.parametrize("value", ["CA1072", "NA12878", "Intellectual disability"])
def test_yaml_str_leaves_plain_strings_alone(value):
    assert yaml_str(value) == value


def test_build_inputs_lays_the_files_out_where_the_samplesheet_points(trio, singleton):
    files = build_inputs([trio, singleton], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    assert set(files) == {
        "samplesheet.csv",
        "pedigrees/CA1072.ped",
        "pedigrees/CA8.ped",
        "phenotypes/CA1072.yml",
        "phenotypes/CA8.yml",
    }
