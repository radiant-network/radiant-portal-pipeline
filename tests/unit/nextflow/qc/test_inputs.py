import csv
import io
import logging

import pytest

from radiant.tasks.nextflow.qc.inputs import SAMPLESHEET_COLUMNS, build_inputs, build_samplesheet
from radiant.tasks.nextflow.qc.resolve import resolve_cases, select_cases

from .conftest import BUCKET

MOUNT = "/workspace/inputs"


@pytest.fixture
def cases(trio_rows, singleton_rows):
    selection = select_cases(trio_rows + singleton_rows)
    return resolve_cases([m.model_dump() for m in selection.members])


def _rows(sheet: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(sheet)))


def test_columns_match_the_pipeline_schema(cases):
    sheet = build_samplesheet(cases, BUCKET, MOUNT)
    assert sheet.splitlines()[0] == ",".join(SAMPLESHEET_COLUMNS)


def test_sample_is_the_aliquot_and_family_is_the_case(cases):
    """DRAGEN names its metric files after the aliquot and the pipeline matches on the exact
    first dot-token, so nothing else can go in `sample`."""
    rows = _rows(build_samplesheet(cases, BUCKET, MOUNT))
    trio = [r for r in rows if r["familyId"] == "CA1072"]
    assert [r["sample"] for r in trio] == ["NA12878", "NA12891", "NA12892"]
    assert [r["participant"] for r in trio] == ["P100", "P101", "P102"]


def test_files_are_pod_paths_not_s3_uris(cases):
    rows = _rows(build_samplesheet(cases, BUCKET, MOUNT))
    assert rows[0]["fileType"] == "CRAM"
    assert rows[0]["file1"].startswith(f"{MOUNT}/dragen/") and rows[0]["file1"].endswith(".cram")
    assert rows[0]["file2"].endswith(".cram.crai")
    assert not any("s3://" in v for r in rows for v in r.values())


def test_pedigree_columns_use_the_pipeline_vocabulary(cases):
    rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT))}
    assert rows["NA12878"]["relationship_to_proband"] == "Proband"
    assert rows["NA12891"]["relationship_to_proband"] == "Father"
    assert rows["NA12892"]["affected_status"] == "Unaffected"
    assert rows["NA12891"]["sex"] == "Male"
    assert rows["NA12878"]["sex"] == "Female"
    assert {r["status"] for r in rows.values()} == {"0"}


def test_strategy_is_per_row(cases):
    rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT))}
    assert rows["NA12878"]["experimentalStrategy"] == "WGS"
    assert rows["HG00096"]["experimentalStrategy"] == "WXS"


def test_dragen_metrics_is_the_tarball_s_pod_path(cases):
    rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT))}
    assert rows["NA12878"]["dragenMetrics"] == f"{MOUNT}/dragen/run-42/NA12878/NA12878.extra.tgz"


def test_without_a_tarball_dragen_metrics_is_the_cram_s_directory(cases, caplog):
    with caplog.at_level(logging.WARNING):
        rows = {r["sample"]: r for r in _rows(build_samplesheet(cases, BUCKET, MOUNT))}
    assert rows["HG00096"]["dragenMetrics"] == f"{MOUNT}/dragen/run-43/cram"
    assert "case 8, aliquot HG00096: no DRAGEN metrics tarball" in caplog.text
    assert "NA12878" not in caplog.text


def test_dragen_metrics_is_never_empty(cases):
    """An empty value makes the pipeline recompute BAM_QC / VCF_QC for that sample."""
    assert all(r["dragenMetrics"] for r in _rows(build_samplesheet(cases, BUCKET, MOUNT)))


def test_a_cram_outside_the_workspace_bucket_fails_here_not_in_the_pod(cases):
    with pytest.raises(ValueError, match="not in the workspace bucket"):
        build_samplesheet(cases, "s3://elsewhere", MOUNT)


def test_a_tarball_outside_the_workspace_bucket_fails_here_too(cases):
    cases[1].proband.dragen_metrics_url = "s3://elsewhere/NA12878.extra.tgz"
    with pytest.raises(ValueError, match="not in the workspace bucket"):
        build_samplesheet(cases, BUCKET, MOUNT)


def test_the_only_input_is_the_samplesheet(cases):
    assert list(build_inputs(cases, BUCKET, MOUNT)) == ["samplesheet.csv"]
