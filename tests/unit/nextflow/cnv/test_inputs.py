import csv
import io

import pytest
import yaml

from radiant.tasks.nextflow.cnv.inputs import build_inputs, build_ped, build_phenopacket, build_samplesheet
from radiant.tasks.nextflow.cnv.resolve import resolve_families

INPUTS_ROOT = "s3://qlin-nextflow-inputs"
INPUTS_MOUNT = "/workspace/inputs"
PREFIX_POD = "/workspace/inputs/cnv-runs/scheduled-2026-09-18T00-00-00-00-00"


@pytest.fixture
def trio(trio_rows, phenotype_rows):
    return resolve_families(trio_rows, phenotype_rows)[0]


@pytest.fixture
def singleton(singleton_rows):
    return resolve_families(singleton_rows, [])[0]


def _rows(csv_text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(csv_text)))


def test_samplesheet_columns_are_the_ones_nf_schema_expects(trio):
    header = build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT).splitlines()[0]
    assert header == "familyId,sample,sequencingType,caller,vcf,cram,pheno,familyPheno,familyPed"


def test_samplesheet_references_pod_paths_not_s3_uris(trio):
    csv_text = build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    assert "s3://" not in csv_text
    row = _rows(csv_text)[0]
    assert row["familyId"] == "CA1072"
    assert row["sample"] == "NA12878"
    assert row["sequencingType"] == "WGS"
    assert row["caller"] == "DRAGEN"
    assert row["vcf"] == f"{INPUTS_MOUNT}/dragen/run-42/NA12878/NA12878.cnv.vcf.gz"
    assert row["cram"] == f"{INPUTS_MOUNT}/dragen/run-42/cram/NA12878.cram"
    assert row["familyPheno"] == f"{PREFIX_POD}/phenotypes/CA1072.yml"
    assert row["familyPed"] == f"{PREFIX_POD}/pedigrees/CA1072.ped"


def test_every_row_takes_the_family_route(trio, singleton):
    """`familyPed` and `familyPheno` on every row, `pheno` on none: even a singleton goes
    through the pipeline's family route, so every output is named after the familyId."""
    rows = _rows(build_samplesheet([trio, singleton], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT))
    assert len(rows) == 4
    assert all(r["pheno"] == "" for r in rows)
    assert all(r["familyPed"] and r["familyPheno"] for r in rows)
    assert rows[3]["familyPed"] == f"{PREFIX_POD}/pedigrees/CA8.ped"


def test_crams_are_omitted_for_the_whole_family_when_one_member_lacks_one(trio):
    """Partial CRAMs would make the pipeline skip refinement anyway; an index not at
    `<cram>.crai` would make it fail. Either way the whole column is left empty."""
    trio.members[2].cram_url = None
    trio.members[2].crai_url = None
    rows = _rows(build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT))
    assert [r["cram"] for r in rows] == ["", "", ""]


def test_a_singleton_without_a_cram_has_an_empty_cram_column(singleton):
    (row,) = _rows(build_samplesheet([singleton], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT))
    assert row["cram"] == ""
    assert row["sequencingType"] == "WES"


def test_a_vcf_outside_the_workspace_bucket_is_rejected(trio):
    trio.members[0].gcnv_url = "s3://some-other-bucket/x.cnv.vcf.gz"
    with pytest.raises(ValueError, match="not in the workspace bucket"):
        build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)


def test_ids_are_the_aliquot_not_the_submitter_sample_id(trio):
    """Same rule as post-processing: the ids must match the VCF sample names."""
    csv_text = build_samplesheet([trio], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    for text in (csv_text, build_ped(trio), build_phenopacket(trio)):
        assert "S-NA" not in text
        assert "NA12878" in text


def test_ped_and_phenopacket_are_the_post_processing_ones(trio):
    """`CnvFamily` is a `Family`, so the annotation DAG's writers apply unchanged."""
    assert build_ped(trio).splitlines() == [
        "CA1072\tNA12878\tNA12891\tNA12892\t2\t2",
        "CA1072\tNA12891\t0\t0\t1\t2",
        "CA1072\tNA12892\t0\t0\t2\t1",
    ]
    doc = yaml.safe_load(build_phenopacket(trio))
    assert doc["id"] == "CA1072"
    assert doc["proband"]["subject"] == {"id": "NA12878", "sex": "FEMALE"}
    assert [f["type"]["id"] for f in doc["proband"]["phenotypicFeatures"]] == ["HP:0001249", "HP:0000618"]
    assert len(doc["pedigree"]["persons"]) == 3


def test_build_inputs_lays_the_files_out_where_the_samplesheet_points(trio, singleton):
    files = build_inputs([trio, singleton], PREFIX_POD, INPUTS_ROOT, INPUTS_MOUNT)
    assert set(files) == {
        "samplesheet.csv",
        "pedigrees/CA1072.ped",
        "pedigrees/CA8.ped",
        "phenotypes/CA1072.yml",
        "phenotypes/CA8.yml",
    }
