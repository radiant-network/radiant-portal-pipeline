import json
import logging

import pytest

from radiant.tasks.nextflow.qc.outputs import (
    MissingOutputsError,
    check_tumor_metrics,
    collect,
    expected_keys,
    has_mapping_metrics,
    kept_cases,
    tumor_metrics_keys,
)
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


def _metrics_json(mapping_metrics=None, **aln_metrics) -> bytes:
    """The shape the pipeline writes, cut to what matters here. `mapping_metrics=None` leaves the
    key out, as the pipeline does when no `<aliquot>.mapping_metrics.csv` was staged."""
    aln = {"total_reads": None, "mean_autosome_coverage": None, "pct_autosomes_15x": None} | aln_metrics
    dragen = {"wgs_coverage_metrics": {"Average autosomal coverage over genome": 41.2}}
    if mapping_metrics is not None:
        dragen = {"mapping_metrics": mapping_metrics} | dragen
    return json.dumps(
        {
            "biosample": {"id": "TCRBOA6-T"},
            "qc_metrics": {"aln_metrics": aln, "variant_metrics": {}},
            "pedigree": {},
            "dragen_metrics": dragen,
        }
    ).encode()


MAPPED = {"Total input reads": 812345678, "Mapped reads": 809876543}


@pytest.fixture
def run(trio_rows, singleton_rows, tumor_rows):
    """Two germline cases and a tumor-only one, whose complete output set was collected."""
    cases = resolve_cases([m.model_dump() for m in select_cases(trio_rows + singleton_rows + tumor_rows).members])
    listing = {key: 4096 for case in cases for key, _ in expected_keys(case).values()}
    return cases, collect(cases, listing, OUTDIR)


def test_only_the_tumor_metrics_json_is_read(run):
    cases, _ = run
    assert tumor_metrics_keys(cases) == {"CA23": "multiqc/CA23/qc_json/TCRBOA6-T.metrics.json"}


def test_a_tumor_with_mapping_metrics_is_kept(run):
    cases, collected = run
    kept = check_tumor_metrics(cases, collected, {"CA23": _metrics_json(MAPPED, total_reads=812345678.0)})
    assert kept == collected
    assert [c.case_id for c in kept_cases(cases, kept)] == [8, 23, 1072]


def test_a_tumor_without_mapping_metrics_is_dropped_and_logged(run, caplog):
    """The tarball held no mapping CSV named after the aliquot: a green run, an empty report."""
    cases, collected = run
    with caplog.at_level(logging.ERROR):
        kept = check_tumor_metrics(cases, collected, {"CA23": _metrics_json()})
    assert sorted(kept) == ["CA1072", "CA8"]
    assert kept["CA1072"] == collected["CA1072"]
    assert [c.case_id for c in kept_cases(cases, kept)] == [8, 1072]
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "case 23, aliquot TCRBOA6-T: no DRAGEN mapping metrics" in record.getMessage()


def test_coverage_values_without_mapping_metrics_are_not_enough(run):
    """`aln_metrics` carries coverage fields filled from `wgs_coverage_metrics.csv` alone."""
    cases, collected = run
    content = _metrics_json(mean_autosome_coverage=41.2, pct_autosomes_15x=0.97, mad_autosome_coverage=3.1)
    assert "CA23" not in check_tumor_metrics(cases, collected, {"CA23": content})


def test_an_empty_mapping_section_is_dropped(run):
    cases, collected = run
    assert "CA23" not in check_tumor_metrics(cases, collected, {"CA23": _metrics_json({})})


def test_germline_cases_are_never_checked(run, caplog):
    """No metrics.json is read for them, and none passed means nothing to drop."""
    cases, collected = run
    germline = [c for c in cases if not c.is_somatic]
    with caplog.at_level(logging.ERROR):
        assert check_tumor_metrics(germline, collected, {}) == collected
    assert caplog.records == []


@pytest.mark.parametrize(
    "content",
    [
        None,
        b"",
        b"not json",
        b"[]",
        b"{}",
        b'{"dragen_metrics": {}}',
        b'{"dragen_metrics": null}',
        b'{"dragen_metrics": {"mapping_metrics": null}}',
        b'{"dragen_metrics": {"mapping_metrics": []}}',
        b'{"dragen_metrics": {"mapping_metrics": {}}}',
    ],
)
def test_missing_non_object_or_empty_mapping_metrics_fail(content):
    assert has_mapping_metrics(content) is False


def test_a_non_empty_mapping_section_passes():
    assert has_mapping_metrics(_metrics_json(MAPPED)) is True


@pytest.fixture
def run_listing(trio_rows, singleton_rows, tumor_rows):
    """Two germline cases and a tumor-only one, and a listing holding every expected file."""
    cases = resolve_cases([m.model_dump() for m in select_cases(trio_rows + singleton_rows + tumor_rows).members])
    return cases, {key: 4096 for case in cases for key, _ in expected_keys(case).values()}


def _without_family(listing, family_id):
    return {key: size for key, size in listing.items() if not key.startswith(f"multiqc/{family_id}/")}


def test_on_the_last_attempt_a_tumor_with_no_report_is_dropped_and_logged(run_listing, caplog):
    """No DRAGEN file named after the aliquot: the pipeline publishes nothing for the family."""
    cases, listing = run_listing
    with caplog.at_level(logging.ERROR):
        collected = collect(cases, _without_family(listing, "CA23"), OUTDIR, last_attempt=True)
    assert sorted(collected) == ["CA1072", "CA8"]
    assert [c.case_id for c in kept_cases(cases, collected)] == [8, 1072]
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "case 23, aliquot TCRBOA6-T: no report" in record.getMessage()


def test_on_the_last_attempt_a_tumor_missing_one_file_is_dropped(run_listing):
    cases, listing = run_listing
    del listing["multiqc/CA23/qc_json/TCRBOA6-T.metrics.json"]
    assert "CA23" not in collect(cases, listing, OUTDIR, last_attempt=True)


def test_before_the_last_attempt_a_missing_tumor_report_keeps_the_polling_going(run_listing):
    """The FSx export can lag the driver pod's exit."""
    cases, listing = run_listing
    with pytest.raises(MissingOutputsError, match="CA23"):
        collect(cases, _without_family(listing, "CA23"), OUTDIR)


def test_on_the_last_attempt_a_missing_germline_report_still_fails_the_run(run_listing):
    cases, listing = run_listing
    with pytest.raises(MissingOutputsError, match="CA1072_multiqc_report.html"):
        collect(cases, _without_family(listing, "CA1072"), OUTDIR, last_attempt=True)


def test_on_the_last_attempt_a_complete_tumor_set_is_collected(run_listing):
    cases, listing = run_listing
    assert sorted(collect(cases, listing, OUTDIR, last_attempt=True)) == ["CA1072", "CA23", "CA8"]
