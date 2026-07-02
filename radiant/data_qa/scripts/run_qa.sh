#!/usr/bin/env bash
# Reusable QA core — assumes StarRocks is already reachable (local tunnel,
# VPN, or in-cluster runner). Runs dbt test and converts the results to
# JUnit XML. Reused verbatim in CI: no tunnel or scheduling logic here.
#
# Exit 0 when the JUnit report was produced (data-test failures are encoded
# in the XML, not treated as a mechanism failure). Exit non-zero only when
# the run itself could not execute (e.g. no connection).
set -uo pipefail

DATA_QA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DATA_QA_DIR"

# venv is optional — in CI, deps may be installed globally.
[[ -d .venv ]] && source .venv/bin/activate

# Load .env locally; in CI the same vars come from the secret store.
if [[ -f .env ]]; then set -a; source .env; set +a; fi
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DATA_QA_DIR}"

mkdir -p reports

# Force a fresh artifact so a hard connection failure is detectable below.
rm -f target/run_results.json

# Don't abort on test failures — we still want to build the report.
dbt test || true

if [[ ! -f target/run_results.json ]]; then
  echo "ERROR: dbt produced no run_results.json — likely a connection/setup failure." >&2
  exit 1
fi

python scripts/run_results_to_junit.py target/run_results.json reports/junit.xml
