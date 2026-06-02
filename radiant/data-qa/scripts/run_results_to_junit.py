#!/usr/bin/env python3
"""Convert a dbt run_results.json artifact into a JUnit XML report.

Standard-library only, so it runs anywhere dbt runs. Reused verbatim in CI:
it takes the dbt artifact and emits JUnit XML that test dashboards
(TestQuality, CI test reporters, ...) can ingest.

Usage:
    python run_results_to_junit.py [run_results.json] [junit.xml]

Defaults: target/run_results.json -> reports/junit.xml

Exit codes:
    0  report written (regardless of whether data tests passed or failed —
       the pass/fail status of each test is encoded in the XML)
    1  run_results.json missing/unreadable (dbt likely failed before
       producing results, e.g. a connection error)
"""
from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path


def _test_name(unique_id: str) -> str:
    # dbt test unique_id: "test.<project>.<test_name>.<hash>".
    # Test names contain no dots (underscores only), so segment 2 is the name.
    parts = unique_id.split(".")
    if len(parts) >= 3 and parts[0] == "test":
        return parts[2]
    return unique_id


def convert(run_results_path: Path, junit_path: Path) -> int:
    if not run_results_path.exists():
        print(
            f"ERROR: {run_results_path} not found — dbt likely failed before "
            f"producing results (connection/setup error).",
            file=sys.stderr,
        )
        return 1

    data = json.loads(run_results_path.read_text())
    results = data.get("results", [])
    project = data.get("metadata", {}).get("project_name") or "radiant_data_qa"
    elapsed = data.get("elapsed_time", 0.0)

    counts = {"fail": 0, "error": 0, "skipped": 0, "warn": 0}

    testsuite = ET.Element("testsuite")
    testsuite.set("name", project)
    testsuite.set("timestamp", datetime.now(timezone.utc).isoformat())

    for r in results:
        status = (r.get("status") or "").lower()
        case = ET.SubElement(testsuite, "testcase")
        case.set("name", _test_name(r.get("unique_id", "unknown")))
        case.set("classname", project)
        case.set("time", f"{r.get('execution_time', 0.0):.3f}")

        message = r.get("message") or ""
        failures = r.get("failures")
        detail = f"{message} (failing rows: {failures})".strip() if failures is not None else message

        if status == "fail":
            counts["fail"] += 1
            el = ET.SubElement(case, "failure")
            el.set("message", message or "test failed")
            el.text = detail
        elif status == "error":
            counts["error"] += 1
            el = ET.SubElement(case, "error")
            el.set("message", message or "test error")
            el.text = detail
        elif status == "skipped":
            counts["skipped"] += 1
            ET.SubElement(case, "skipped")
        elif status == "warn":
            counts["warn"] += 1
            so = ET.SubElement(case, "system-out")
            so.text = f"WARN: {detail}"
        # "pass" -> bare <testcase/>

    total = len(results)
    attrs = {
        "name": project,
        "tests": str(total),
        "failures": str(counts["fail"]),
        "errors": str(counts["error"]),
        "skipped": str(counts["skipped"]),
        "time": f"{elapsed:.3f}",
    }
    for k, v in attrs.items():
        testsuite.set(k, v)

    testsuites = ET.Element("testsuites")
    for k, v in attrs.items():
        testsuites.set(k, v)
    testsuites.append(testsuite)

    tree = ET.ElementTree(testsuites)
    ET.indent(tree, space="  ")
    junit_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(junit_path, encoding="utf-8", xml_declaration=True)

    print(
        f"Wrote {junit_path}: {total} tests, "
        f"{counts['fail']} failures, {counts['error']} errors, "
        f"{counts['skipped']} skipped, {counts['warn']} warnings."
    )
    return 0


def main() -> int:
    args = sys.argv[1:]
    run_results = Path(args[0]) if len(args) >= 1 else Path("target/run_results.json")
    junit = Path(args[1]) if len(args) >= 2 else Path("reports/junit.xml")
    return convert(run_results, junit)


if __name__ == "__main__":
    raise SystemExit(main())
