import csv
import io
import logging

import pytest

from radiant.tasks.nextflow.qc.inputs import SAMPLESHEET_COLUMNS, build_inputs, build_samplesheet
from radiant.tasks.nextflow.qc.resolve import resolve_cases, select_cases

from .conftest import BUCKET

MOUNT = "/workspace/inputs"
PLACEHOLDER = "/workspace/reference/qc/placeholder.vcf.gz"

# The trio's and the singleton's germline rows, byte for byte: a tumor row must not change them.
GERMLINE_SHEET = """\
participant,sample,familyId,fileType,file1,file2,experimentalStrategy,sex,status,relationship_to_proband,affected_status,dragenMetrics
P200,HG00096,CA8,CRAM,/workspace/inputs/dragen/run-43/cram/HG00096.cram,/workspace/inputs/dragen/run-43/cram/HG00096.cram.crai,WXS,Male,0,Proband,Affected,/workspace/inputs/dragen/run-43/cram
P100,NA12878,CA1072,CRAM,/workspace/inputs/dragen/run-42/cram/NA12878.cram,/workspace/inputs/dragen/run-42/cram/NA12878.cram.crai,WGS,Female,0,Proband,Affected,/workspace/inputs/dragen/run-42/NA12878/NA12878.extra.tgz
P101,NA12891,CA1072,CRAM,/workspace/inputs/dragen/run-42/cram/NA12891.cram,/workspace/inputs/dragen/run-42/cram/NA12891.cram.crai,WGS,Male,0,Father,Affected,/workspace/inputs/dragen/run-42/NA12891/NA12891.extra.tgz
P102,NA12892,CA1072,CRAM,/workspace/inputs/dragen/run-42/cram/NA12892.cram,/workspace/inputs/dragen/run-42/cram/NA12892.cram.crai,WGS,Female,0,Mother,Unaffected,/workspace/inputs/dragen/run-42/NA12892/NA12892.extra.tgz
"""  # noqa: E501


def _resolve(rows):
    return resolve_cases([m.model_dump() for m in select_cases(rows).members])


@pytest.fixture
def cases(trio_rows, singleton_rows):
    return _resolve(trio_rows + singleton_rows)


@pytest.fixture
def mixed_cases(trio_rows, singleton_rows, tumor_rows):
    return _resolve(trio_rows + singleton_rows + tumor_rows)


def _rows(sheet: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(sheet)))


def test_columns_match_the_pipeline_schema(cases):
    sheet = build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER)
    assert sheet.splitlines()[0] == ",".join(SAMPLESHEET_COLUMNS)


def test_sample_is_the_aliquot_and_family_is_the_case(cases):
    """DRAGEN names its metric files after the aliquot and the pipeline matches on the exact
    first dot-token, so nothing else can go in `sample`."""
    rows = _rows(build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER))
    trio = [r for r in rows if r["familyId"] == "CA1072"]
    assert [r["sample"] for r in trio] == ["NA12878", "NA12891", "NA12892"]
    assert [r["participant"] for r in trio] == ["P100", "P101", "P102"]


def test_files_are_pod_paths_not_s3_uris(cases):
    rows = _rows(build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER))
    assert rows[0]["fileType"] == "CRAM"
    assert rows[0]["file1"].startswith(f"{MOUNT}/dragen/") and rows[0]["file1"].endswith(".cram")
    assert rows[0]["file2"].endswith(".cram.crai")
    assert not any("s3://" in v for r in rows for v in r.values())


def test_pedigree_columns_use_the_pipeline_vocabulary(cases):
    rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER))}
    assert rows["NA12878"]["relationship_to_proband"] == "Proband"
    assert rows["NA12891"]["relationship_to_proband"] == "Father"
    assert rows["NA12892"]["affected_status"] == "Unaffected"
    assert rows["NA12891"]["sex"] == "Male"
    assert rows["NA12878"]["sex"] == "Female"
    assert {r["status"] for r in rows.values()} == {"0"}


def test_strategy_is_per_row(cases):
    rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER))}
    assert rows["NA12878"]["experimentalStrategy"] == "WGS"
    assert rows["HG00096"]["experimentalStrategy"] == "WXS"


def test_dragen_metrics_is_the_tarball_s_pod_path(cases):
    rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER))}
    assert rows["NA12878"]["dragenMetrics"] == f"{MOUNT}/dragen/run-42/NA12878/NA12878.extra.tgz"


def test_without_a_tarball_dragen_metrics_is_the_cram_s_directory(cases, caplog):
    with caplog.at_level(logging.WARNING):
        rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER))}
    assert rows["HG00096"]["dragenMetrics"] == f"{MOUNT}/dragen/run-43/cram"
    assert "case 8, aliquot HG00096: no DRAGEN metrics tarball" in caplog.text
    assert "NA12878" not in caplog.text


def test_dragen_metrics_is_never_empty(cases):
    """An empty value makes the pipeline recompute BAM_QC / VCF_QC for that sample."""
    assert all(r["dragenMetrics"] for r in _rows(build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER)))


def test_a_cram_outside_the_workspace_bucket_fails_here_not_in_the_pod(cases):
    with pytest.raises(ValueError, match="not in the workspace bucket"):
        build_samplesheet(cases, "s3://elsewhere", MOUNT, PLACEHOLDER)


def test_a_tarball_outside_the_workspace_bucket_fails_here_too(cases):
    cases[1].proband.dragen_metrics_url = "s3://elsewhere/NA12878.extra.tgz"
    with pytest.raises(ValueError, match="not in the workspace bucket"):
        build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER)


def test_the_only_input_is_the_samplesheet(cases):
    assert list(build_inputs(cases, BUCKET, MOUNT, PLACEHOLDER)) == ["samplesheet.csv"]


def test_germline_rows_are_unchanged_byte_for_byte(cases):
    assert build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER) == GERMLINE_SHEET


def test_a_tumor_is_a_vcf_row_on_the_placeholder_with_its_tarball(mixed_cases):
    """In DRAGEN mode the pipeline only checks that a VCF row's file exists and runs somalier on
    CRAM / BAM rows only: the tumor gets its DRAGEN sections and no somalier."""
    tumor = next(
        r for r in _rows(build_samplesheet(mixed_cases, BUCKET, MOUNT, PLACEHOLDER)) if r["familyId"] == "CA23"
    )
    assert tumor == {
        "participant": "P62",
        "sample": "TCRBOA6-T",
        "familyId": "CA23",
        "fileType": "VCF",
        "file1": PLACEHOLDER,
        "file2": "",
        "experimentalStrategy": "WGS",
        "sex": "Male",
        "status": "1",
        "relationship_to_proband": "Proband",
        "affected_status": "Unknown",
        "dragenMetrics": f"{MOUNT}/dragen/run-44/TCRBOA6-T/TCRBOA6-T.extra.tgz",
    }


def test_germline_rows_are_unchanged_next_to_a_tumor(mixed_cases):
    """One samplesheet, one launcher run: the tumor row is added, the germline rows stay as they were."""
    lines = build_samplesheet(mixed_cases, BUCKET, MOUNT, PLACEHOLDER).splitlines(keepends=True)
    assert "".join(line for line in lines if ",CA23," not in line) == GERMLINE_SHEET
    assert len(lines) == len(GERMLINE_SHEET.splitlines()) + 1


def test_the_placeholder_is_taken_as_is_not_mapped_from_the_inputs_bucket(tumor_rows):
    """It lives on the reference mount, so it is already a pod path."""
    sheet = build_samplesheet(_resolve(tumor_rows), BUCKET, MOUNT, "/elsewhere/empty.vcf.gz")
    assert _rows(sheet)[0]["file1"] == "/elsewhere/empty.vcf.gz"


def test_a_tumor_without_a_tarball_fails_instead_of_reading_its_cram_directory(tumor_rows):
    """`select_cases` excludes it as `no_metrics`; the directory fallback would give a green run
    with an empty report."""
    cases = _resolve(tumor_rows)
    cases[0].proband.dragen_metrics_url = None
    with pytest.raises(ValueError, match="a tumor row needs a DRAGEN metrics tarball"):
        build_samplesheet(cases, BUCKET, MOUNT, PLACEHOLDER)
