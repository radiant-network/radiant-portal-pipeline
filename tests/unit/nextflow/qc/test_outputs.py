import pytest

from radiant.tasks.nextflow.qc.outputs import MissingOutputsError, collect, expected_keys
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
