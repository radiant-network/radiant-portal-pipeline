import pytest
import yaml

from radiant.tasks.nextflow.inputs import build_inputs, build_ped, build_phenopacket, build_samplesheet
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
    """Exomiser ranks on observed terms; excluded ones only penalise. Order is meaning, so
    it is asserted on the parsed sequence rather than on where strings land in the text."""
    features = yaml.safe_load(build_phenopacket(trio))["proband"]["phenotypicFeatures"]
    assert [f["type"]["id"] for f in features] == ["HP:0001249", "HP:0000618"]
    assert "excluded" not in features[0]
    assert features[1]["excluded"] is True


def test_phenopacket_carries_the_pedigree_and_the_proband(trio):
    doc = yaml.safe_load(build_phenopacket(trio))
    assert doc["id"] == "CA1072"
    assert doc["proband"]["subject"] == {"id": "NA12878", "sex": "FEMALE"}
    assert doc["pedigree"]["persons"] == [
        {
            "individualId": "NA12878",
            "paternalId": "NA12891",
            "maternalId": "NA12892",
            "sex": "FEMALE",
            "affectedStatus": "AFFECTED",
        },
        {"individualId": "NA12891", "sex": "MALE", "affectedStatus": "AFFECTED"},
        {"individualId": "NA12892", "sex": "FEMALE", "affectedStatus": "UNAFFECTED"},
    ]
    assert doc["metaData"]["phenopacketSchemaVersion"] == 2.0
    assert doc["metaData"]["resources"][0]["iriPrefix"] == "http://purl.obolibrary.org/obo/HP_"


def test_phenopacket_omits_the_features_block_when_there_are_no_terms(singleton):
    assert "phenotypicFeatures" not in yaml.safe_load(build_phenopacket(singleton))["proband"]


def test_phenopacket_keys_stay_in_the_formats_own_order(trio):
    """Alphabetising would put metaData before proband. Valid, but it stops reading like a
    phenopacket, so `sort_keys=False` is deliberate."""
    assert list(yaml.safe_load(build_phenopacket(trio))) == ["id", "proband", "pedigree", "metaData"]


def test_an_all_digit_sample_id_survives_as_a_string(trio):
    """Phenopacket ids are string fields. Unquoted, `1072` would load back as an int --
    which is the same coercion that makes a bare numeric familyId fail nf-schema."""
    trio.members[0].sample_id = "1072"
    doc = yaml.safe_load(build_phenopacket(trio))
    assert doc["proband"]["subject"]["id"] == "1072"
    assert doc["pedigree"]["persons"][0]["individualId"] == "1072"


def test_build_inputs_lays_the_files_out_where_the_samplesheet_points(trio, singleton):
    files = build_inputs([trio, singleton], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    assert set(files) == {
        "samplesheet.csv",
        "pedigrees/CA1072.ped",
        "pedigrees/CA8.ped",
        "phenotypes/CA1072.yml",
        "phenotypes/CA8.yml",
    }
