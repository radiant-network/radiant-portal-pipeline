#!/usr/bin/env python3
"""Run the dbt suite once for the shared tables, then once per tenant, and merge the results.

dbt resolves ``source.schema`` at parse time — one value per invocation — so a single
``dbt test`` cannot fan the per-tenant sources across N tenant databases. This module runs
1 + N passes and merges their artifacts into the single ``target/run_results.json`` +
``reports/junit.xml`` pair that ``run_qa.sh`` promises and the Airflow DAG consumes:

    pass 0  shared   dbt test --exclude source:tenant_db
    pass i  <tenant> dbt test --select  source:tenant_db   (SR_TENANT_SCHEMA=<schema>)

The two selectors are complementary and disjoint thanks to dbt's default *eager* indirect
selection, so every test node runs in exactly one pass. Tests that join a per-tenant table to
a base table land in the tenant pass, which is where they belong.

Tenants come from the ``TENANTS`` env var, as JSON emitted by the DAG:

    TENANTS=[{"code": "chusj", "schema": "chusj_tenant"}, ...]

Each tenant's *database* is resolved Airflow-side (``RADIANT_TENANT_DB_TEMPLATE`` is
configurable), so this module never rebuilds the name itself. Absent or empty ``TENANTS``
means "shared pass only" — the behaviour of a plain local run.

Exit codes match run_qa.sh's original contract:
    0  reports written (data-test failures are encoded in the XML, not treated as a
       mechanism failure)
    1  the shared pass could not run at all (e.g. no StarRocks connection)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from run_results_to_junit import convert

DATA_QA_DIR = Path(__file__).resolve().parent.parent

# dbt source name carrying the tables that live in {tenant}_tenant. Keep in sync with
# the `- name:` header of the per-tenant files in sources/.
TENANT_SOURCE = "source:tenant_db"


@dataclass(frozen=True)
class Pass:
    """One `dbt test` invocation."""

    label: str
    # None marks the shared/base pass; a tenant code otherwise. This is what gets stamped
    # onto every result so the merged report can say *which* tenant failed.
    tenant: str | None
    schema: str
    select_args: list[str] = field(default_factory=list)


def plan_passes(tenants_json: str | None, base_schema: str) -> list[Pass]:
    """Build the 1 + N pass list from the TENANTS env var."""
    passes = [Pass("shared", None, base_schema, ["--exclude", TENANT_SOURCE])]

    tenants = json.loads(tenants_json) if (tenants_json or "").strip() else []
    if not isinstance(tenants, list):
        raise ValueError(f"TENANTS must be a JSON list, got {type(tenants).__name__}.")

    for entry in tenants:
        try:
            code, schema = entry["code"], entry["schema"]
        except (TypeError, KeyError) as exc:
            raise ValueError(
                f"Each TENANTS entry needs a 'code' and a 'schema', got: {entry!r}. "
                "The DAG resolves the schema via RADIANT_TENANT_DB_TEMPLATE."
            ) from exc
        passes.append(Pass(code, code, schema, ["--select", TENANT_SOURCE]))

    return passes


def _invocation_error(qa_pass: Pass, message: str) -> dict:
    """A synthetic result standing in for a pass that never produced an artifact.

    Without it a tenant whose dbt invocation died would silently vanish from the report and
    the DAG would go green. The id carries no SJRA tag, so check_results counts it as an
    unexpected failure.
    """
    return {
        "unique_id": f"test.radiant_data_qa.dbt_invocation_failed__{qa_pass.label}",
        "status": "error",
        "message": message,
        "execution_time": 0.0,
        "failures": None,
        "tenant": qa_pass.tenant,
    }


def run_pass(qa_pass: Pass, env: dict, data_qa_dir: Path = DATA_QA_DIR) -> dict | None:
    """Run one `dbt test` and return its parsed run_results.json, or None if it produced none."""
    run_results = data_qa_dir / "target" / "run_results.json"

    # Delete before *every* pass, not just the first. There is no root .dockerignore, so
    # `COPY radiant/data_qa/` can bake a stale target/run_results.json into the image; and
    # between passes, a leftover artifact would be misattributed to the next tenant.
    run_results.unlink(missing_ok=True)

    print(f"\n=== dbt test [{qa_pass.label}] schema={qa_pass.schema} ===", flush=True)
    # The return code is deliberately ignored: a failing data test is a result, not a
    # mechanism failure. A pass that truly could not run leaves no artifact behind.
    subprocess.run(
        ["dbt", "test", *qa_pass.select_args],
        env={**env, "SR_TENANT_SCHEMA": qa_pass.schema},
        cwd=data_qa_dir,
        check=False,
    )

    if not run_results.exists():
        return None

    artifact = json.loads(run_results.read_text())

    # Keep the raw per-pass artifact around; the merged one loses dbt's own framing.
    fragments = data_qa_dir / "target" / "run_results"
    fragments.mkdir(parents=True, exist_ok=True)
    (fragments / f"{qa_pass.label}.json").write_text(json.dumps(artifact, indent=2))

    return artifact


def merge(pass_artifacts: list[tuple[Pass, dict | None]]) -> dict:
    """Concatenate per-pass artifacts into one, stamping the tenant onto every result."""
    merged: dict = {"metadata": {}, "results": [], "elapsed_time": 0.0}

    for qa_pass, artifact in pass_artifacts:
        if artifact is None:
            merged["results"].append(
                _invocation_error(
                    qa_pass,
                    f"dbt produced no run_results.json for pass '{qa_pass.label}' "
                    f"(schema {qa_pass.schema}) — likely a connection/setup failure.",
                )
            )
            continue

        if not merged["metadata"]:
            merged["metadata"] = artifact.get("metadata", {})
        merged["elapsed_time"] += artifact.get("elapsed_time", 0.0) or 0.0
        merged["results"].extend({**r, "tenant": qa_pass.tenant} for r in artifact.get("results", []))

    return merged


def main(env: dict | None = None, data_qa_dir: Path = DATA_QA_DIR) -> int:
    env = dict(os.environ if env is None else env)
    passes = plan_passes(env.get("TENANTS"), env.get("SR_SCHEMA", "radiant"))

    pass_artifacts: list[tuple[Pass, dict | None]] = []
    for qa_pass in passes:
        artifact = run_pass(qa_pass, env, data_qa_dir)
        if artifact is None and qa_pass.tenant is None:
            # The shared pass could not run at all — the tenant passes use the same
            # connection, so there is nothing left to try. Keeps run_qa.sh's exit-1 contract.
            print(
                "ERROR: the shared dbt pass produced no run_results.json — likely a connection/setup failure.",
                file=sys.stderr,
            )
            return 1
        # A *tenant* pass that dies must not abort the loop: tenant B is still worth testing
        # when tenant A's database is broken. merge() turns the None into an error result.
        pass_artifacts.append((qa_pass, artifact))

    merged = merge(pass_artifacts)
    run_results = data_qa_dir / "target" / "run_results.json"
    run_results.parent.mkdir(parents=True, exist_ok=True)
    run_results.write_text(json.dumps(merged, indent=2))

    print(
        f"\nMerged {len(passes)} dbt pass(es) into {run_results}: {len(merged['results'])} results.",
        flush=True,
    )
    return convert(run_results, data_qa_dir / "reports" / "junit.xml")


if __name__ == "__main__":
    raise SystemExit(main())
