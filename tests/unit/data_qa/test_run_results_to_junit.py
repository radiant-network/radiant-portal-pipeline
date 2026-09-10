"""JUnit conversion of a merged, multi-tenant run_results.json."""

import json
import xml.etree.ElementTree as ET


def _result(unique_id, status="pass", tenant=None, **extra):
    r = {"unique_id": unique_id, "status": status, "execution_time": 0.5, **extra}
    if tenant is not None:
        r["tenant"] = tenant
    return r


def _convert(junit, tmp_path, results, elapsed=1.0):
    src = tmp_path / "run_results.json"
    src.write_text(
        json.dumps({"metadata": {"project_name": "radiant_data_qa"}, "results": results, "elapsed_time": elapsed})
    )
    out = tmp_path / "reports" / "junit.xml"
    assert junit.convert(src, out) == 0
    return ET.parse(out).getroot()


def test_colliding_unique_ids_across_tenants_stay_distinct(junit, tmp_path):
    """The same assertion in two tenants has the same unique_id; only the tenant separates them.

    Without the suffix the two cases collide in the report and "which tenant failed" is lost.
    """
    uid = "test.radiant_data_qa.snv_variant__should_not_be_empty.abc123"
    root = _convert(
        junit,
        tmp_path,
        [
            _result(uid, status="pass", tenant="chusj"),
            _result(uid, status="fail", tenant="chop", message="boom", failures=3),
        ],
    )

    names = [c.get("name") for c in root.iter("testcase")]
    assert names == [
        "snv_variant__should_not_be_empty[tenant=chusj]",
        "snv_variant__should_not_be_empty[tenant=chop]",
    ]
    assert len(set(names)) == 2

    # only the chop case carries the failure
    (failing,) = [c for c in root.iter("testcase") if c.find("failure") is not None]
    assert failing.get("name") == "snv_variant__should_not_be_empty[tenant=chop]"


def test_counts_add_up_across_passes(junit, tmp_path):
    root = _convert(
        junit,
        tmp_path,
        [
            _result("test.radiant_data_qa.shared_one.a", status="pass"),
            _result("test.radiant_data_qa.shared_two.b", status="fail", message="x"),
            _result("test.radiant_data_qa.tenant_one.c", status="fail", tenant="chusj", message="y"),
            _result("test.radiant_data_qa.tenant_one.c", status="error", tenant="chop", message="z"),
            _result("test.radiant_data_qa.tenant_two.d", status="skipped", tenant="chop"),
        ],
    )

    assert root.get("tests") == "5"
    assert root.get("failures") == "2"
    assert root.get("errors") == "1"
    assert root.get("skipped") == "1"


def test_result_without_tenant_renders_unchanged(junit, tmp_path):
    """Shared-pass results, and any pre-existing artifact, must render as they always did."""
    root = _convert(junit, tmp_path, [_result("test.radiant_data_qa.clinvar__should_be_unique__id.9f")])
    (case,) = list(root.iter("testcase"))
    assert case.get("name") == "clinvar__should_be_unique__id"
