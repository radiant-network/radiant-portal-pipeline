import logging

import pytest

from radiant.tasks.nextflow.qc.outputs import MissingOutputsError, collect, expected_keys, kept_cases
from radiant.tasks.nextflow.qc.resolve import resolve_cases, select_cases

OUTDIR = "s3://qlin-nextflow-outputs/scheduled-2026-09-02"


@pytest.fixture
def trio(trio_rows):
    return resolve_cases([m.model_dump() for m in select_cases(trio_rows).members])[0]


@pytest.fixture
def listing(trio):
    return {key: 4096 for key, _ in expected_keys(trio).values()}


def test_the_per_family_multiqc_layout(trio):
    """What a real QA run published under `multiqc/<familyId>/`."""
    keys = {name: key for name, (key, _) in expected_keys(trio).items()}
    assert keys == {
        "multiqc_html": "multiqc/CA1072/CA1072_multiqc_report.html",
        "multiqc_data": "multiqc/CA1072/CA1072_multiqc_report_data.zip",
        "metrics_json_NA12878": "multiqc/CA1072/qc_json/NA12878.metrics.json",
        "metrics_json_NA12891": "multiqc/CA1072/qc_json/NA12891.metrics.json",
        "metrics_json_NA12892": "multiqc/CA1072/qc_json/NA12892.metrics.json",
    }


def test_everything_registers_as_aggqc(trio, listing):
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert {d["data_type_code"] for d in documents.values()} == {"aggqc"}
    assert {d["data_category_code"] for d in documents.values()} == {"genomic"}
    assert documents["multiqc_html"]["format_code"] == "html"
    assert documents["multiqc_data"]["format_code"] == "zip"
    assert documents["multiqc_html"]["url"] == f"{OUTDIR}/multiqc/CA1072/CA1072_multiqc_report.html"
    assert documents["multiqc_html"]["name"] == "CA1072_multiqc_report.html"


def test_each_member_metrics_json_is_registered(trio, listing):
    listing["multiqc/CA1072/qc_json/NA12891.metrics.json"] = 5120
    assert collect([trio], listing, OUTDIR)["CA1072"]["metrics_json_NA12891"] == {
        "name": "NA12891.metrics.json",
        "url": f"{OUTDIR}/multiqc/CA1072/qc_json/NA12891.metrics.json",
        "size": 5120,
        "data_category_code": "genomic",
        "data_type_code": "aggqc",
        "format_code": "json",
    }


def test_sizes_come_from_the_listing(trio, listing):
    listing["multiqc/CA1072/CA1072_multiqc_report.html"] = 3772915
    assert collect([trio], listing, OUTDIR)["CA1072"]["multiqc_html"]["size"] == 3772915


def test_a_missing_archive_refuses_the_whole_case(trio, listing):
    del listing["multiqc/CA1072/CA1072_multiqc_report_data.zip"]
    with pytest.raises(MissingOutputsError, match="CA1072_multiqc_report_data.zip"):
        collect([trio], listing, OUTDIR)


def test_a_missing_metrics_json_refuses_the_whole_case(trio, listing):
    del listing["multiqc/CA1072/qc_json/NA12892.metrics.json"]
    with pytest.raises(MissingOutputsError, match="NA12892.metrics.json"):
        collect([trio], listing, OUTDIR)


def test_the_metrics_json_of_a_sample_outside_the_case_is_not_registered(trio, listing):
    """A family member who is not in the case: their JSON can sit in the same folder."""
    listing["multiqc/CA1072/qc_json/NA12877.metrics.json"] = 4096
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert "NA12877.metrics.json" not in {d["name"] for d in documents.values()}
    assert len(documents) == 5


@pytest.fixture
def run_listing(trio_rows, singleton_rows, tumor_rows):
    """Two germline cases and a tumor-only one, and a listing holding every expected file."""
    cases = resolve_cases([m.model_dump() for m in select_cases(trio_rows + singleton_rows + tumor_rows).members])
    return cases, {key: 4096 for case in cases for key, _ in expected_keys(case).values()}


def _without_family(listing, family_id):
    return {key: size for key, size in listing.items() if not key.startswith(f"multiqc/{family_id}/")}


def test_a_tumor_with_no_report_is_dropped_and_logged(run_listing, caplog):
    """No DRAGEN file named after the aliquot: the pipeline publishes nothing for the family."""
    cases, listing = run_listing
    with caplog.at_level(logging.ERROR):
        collected = collect(cases, _without_family(listing, "CA23"), OUTDIR)
    assert sorted(collected) == ["CA1072", "CA8"]
    assert [c.case_id for c in kept_cases(cases, collected)] == [8, 1072]
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "case 23, aliquot TCRBOA6-T: no report" in record.getMessage()


def test_a_tumor_missing_one_file_is_dropped(run_listing):
    cases, listing = run_listing
    del listing["multiqc/CA23/qc_json/TCRBOA6-T.metrics.json"]
    assert "CA23" not in collect(cases, listing, OUTDIR)


def test_a_missing_germline_report_still_fails_the_run(run_listing):
    cases, listing = run_listing
    with pytest.raises(MissingOutputsError, match="CA1072_multiqc_report.html"):
        collect(cases, _without_family(listing, "CA1072"), OUTDIR)


def test_a_complete_tumor_set_is_collected(run_listing):
    cases, listing = run_listing
    assert sorted(collect(cases, listing, OUTDIR)) == ["CA1072", "CA23", "CA8"]
