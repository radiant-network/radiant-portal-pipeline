#!/usr/bin/env bash
# Reusable QA core — assumes StarRocks is already reachable (local tunnel,
# VPN, or in-cluster runner). Runs the dbt tests and converts the results to
# JUnit XML. Reused verbatim in CI: no tunnel or scheduling logic here.
#
# This script owns only the environment (venv, .env, profiles dir). The run
# itself lives in scripts/run_qa.py, which invokes dbt once for the shared
# tables and once per tenant listed in $TENANTS, then merges the artifacts.
# Set TENANTS to test per-tenant tables; without it only the shared pass runs.
#
# Exit 0 when the JUnit report was produced (data-test failures are encoded
# in the XML, not treated as a mechanism failure). Exit non-zero only when
# the run itself could not execute (e.g. no connection).
#
# Note: the Airflow pipeline runs this same script automatically. It relies on
# the two files created here (target/run_results.json and reports/junit.xml)
# and on the script exit code. If you change any of those, the pipeline needs
# to be updated too.
set -uo pipefail

DATA_QA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DATA_QA_DIR"

# venv is optional — in CI, deps may be installed globally.
[[ -d .venv ]] && source .venv/bin/activate

# Load .env locally; in CI the same vars come from the secret store.
if [[ -f .env ]]; then set -a; source .env; set +a; fi
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DATA_QA_DIR}"

mkdir -p reports

# Runs dbt 1+N times, merges the artifacts and writes the JUnit report. Owns the
# stale-artifact deletion and the exit codes this script used to handle inline.
python scripts/run_qa.py
