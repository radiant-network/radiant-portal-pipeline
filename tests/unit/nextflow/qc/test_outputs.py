import pytest

from radiant.tasks.nextflow.qc.outputs import MissingOutputsError, collect, expected_keys
from radiant.tasks.nextflow.qc.resolve import resolve_cases, select_cases

OUTDIR = "s3://qlin-nextflow-outputs/scheduled__2026-09-02-g0"


@pytest.fixture
def trio(trio_rows):
    return resolve_cases([m.model_dump() for m in select_cases(trio_rows).members])[0]


@pytest.fixture
def listing(trio):
    return {key: 4096 for key, _ in expected_keys(trio).values()}


def test_the_per_family_multiqc_layout(trio):
    """What a real QA run published under `multiqc/<familyId>/`."""
    keys = {name: key for name, (key, _) in expected_keys(trio).items()}
    assert keys["multiqc_html"] == "multiqc/CA1072/CA1072_multiqc_report.html"
    assert keys["multiqc_data"] == "multiqc/CA1072/CA1072_multiqc_report_data.zip"
    assert keys["metrics_json:NA12891"] == "multiqc/CA1072/qc_json/NA12891.metrics.json"
    assert len(keys) == 5


def test_everything_registers_as_aggqc(trio, listing):
    documents = collect([trio], listing, OUTDIR)["CA1072"]
    assert {d["data_type_code"] for d in documents.values()} == {"aggqc"}
    assert {d["data_category_code"] for d in documents.values()} == {"genomic"}
    assert documents["multiqc_html"]["format_code"] == "html"
    assert documents["multiqc_data"]["format_code"] == "zip"
    assert documents["metrics_json:NA12878"]["format_code"] == "json"
    assert documents["multiqc_html"]["url"] == f"{OUTDIR}/multiqc/CA1072/CA1072_multiqc_report.html"
    assert documents["multiqc_html"]["name"] == "CA1072_multiqc_report.html"


def test_sizes_come_from_the_listing(trio, listing):
    listing["multiqc/CA1072/CA1072_multiqc_report.html"] = 3772915
    assert collect([trio], listing, OUTDIR)["CA1072"]["multiqc_html"]["size"] == 3772915


def test_a_missing_sidecar_refuses_the_whole_case(trio, listing):
    del listing["multiqc/CA1072/qc_json/NA12892.metrics.json"]
    with pytest.raises(MissingOutputsError, match="NA12892.metrics.json"):
        collect([trio], listing, OUTDIR)
