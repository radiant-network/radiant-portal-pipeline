# Radiant Data QA

dbt project for running data-quality assertions against the Radiant
StarRocks database, without producing any models. Generic and singular
tests assert null-safety, uniqueness, accepted values, and cross-field
invariants on tables.

## Test Categories

| Category                       | dbt implementation                              |
| ------------------------------ | ----------------------------------------------- |
| Should Not Contain Null        | `tests: [not_null]` (built-in, displayed via custom `name:`)  |
| Should Not Contain Only Null   | `should_not_contain_only_null` (dynamic, custom)              |
| Should Not Contain Same Value  | `should_not_contain_same_value` (dynamic, custom)             |
| Should Be Unique               | `tests: [unique]` (built-in, displayed via custom `name:`)    |
| Values Contained In Dictionary | `tests: [accepted_values: {values: [...]}]`                   |
| Cross-Field / Custom Invariant | Singular tests in `tests/*.sql`                               |

The two **dynamic** tests (`should_not_contain_only_null`,
`should_not_contain_same_value`) introspect the target table's columns at
compile time and emit one check per column, minus an explicit `except` list.
New columns are picked up on the next run automatically; dropped columns
disappear from the check. Only the `except` list needs maintenance.

For the built-in dbt tests (`not_null`, `unique`), the YAML keeps standard
dbt syntax — only the displayed name is overridden via `name:` so that
TestQuality / JUnit reports show consistent vocabulary
(`should_not_contain_null_<col>`, `should_be_unique_<col>`).

## Prerequisites

- Python 3.10+
- Access to a Radiant StarRocks instance (qa creds)
- StarRocks version 2.5+

## Setup

```bash
cd data-qa
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
dbt deps                     # installs dbt_utils
```

## Configuration

Connection params come from env vars (no secrets in the repo). Copy
`.env.example` to `.env` and fill in the values, or export them in your shell:

```bash
export SR_HOST=localhost
export SR_PORT=9040
export SR_USER=...
export SR_PASSWORD=...
export SR_SCHEMA=radiant
export DBT_PROFILES_DIR=$(pwd)
export DBT_SEND_ANONYMOUS_USAGE_STATS=false
```

## Run

```bash
# Sanity check connection
dbt debug

# Run all data tests
dbt test

# Just the snv__variant tests
dbt test --select source:radiant.snv__variant
```

dbt writes detailed results to `target/run_results.json`. Each failing test
also writes its failing rows to `target/compiled/.../...sql` — useful to
investigate.

## Layout

```
data-qa/
├── dbt_project.yml      # minimal — no models, just tests
├── profiles.yml         # env-var driven, single target
├── packages.yml         # dbt_utils
├── requirements.txt
├── macros/
│   ├── test_should_not_contain_only_null.sql  # dynamic null-coverage sweep
│   └── test_should_not_contain_same_value.sql # dynamic constant-value sweep
├── sources/
│   └── snv.yml          # snv__variant generic tests
├── tests/
│   └── snv_variant__validate_no_star_alternate.sql  # singular test example
├── scripts/
│   ├── run_results_to_junit.py  # run_results.json -> JUnit XML (stdlib)
│   └── run_qa.sh                # reusable core: dbt test + convert (CI-ready)
├── reports/             # generated JUnit XML (gitignored) for TestQuality
└── models/              # empty by design
```

## Adding tests for another table

1. Add a `sources/<name>.yml` entry under `sources[0].tables`.
2. List its columns with the relevant generic tests, and/or attach
   `should_not_contain_only_null` / `should_not_contain_same_value` at the
   table level with an appropriate `except` list.
3. For cross-field or query-shaped checks that don't fit a generic test,
   add a `.sql` file under `tests/` that returns the failing rows.

## Reporting

dbt writes detailed results to `target/run_results.json` after each run.
`scripts/run_results_to_junit.py` converts that artifact into JUnit XML,
the format that test dashboards (TestQuality, CI test reporters, ...) ingest.

### Pieces

- **`scripts/run_results_to_junit.py`** — converts dbt's `run_results.json`
  into `reports/junit.xml`. Stdlib only. Maps dbt status to JUnit:
  `fail → <failure>`, `error → <error>`, `skipped → <skipped>`,
  `warn → <system-out>`, `pass → bare <testcase>`. A data-test failure is
  encoded in the XML, **not** treated as a script failure — the run only
  "fails" if dbt couldn't execute at all (e.g. no connection).
- **`scripts/run_qa.sh`** — the reusable core: `dbt test` + JUnit conversion.
  Assumes StarRocks is already reachable (VPN, tunnel, or in-cluster runner).
  **Reused verbatim in CI.**

### Generating the report

With connectivity to StarRocks established (see Configuration above):

```bash
./scripts/run_qa.sh        # dbt test + writes reports/junit.xml
```

### Import into TestQuality

`reports/junit.xml` is a standard JUnit XML file — upload it through
TestQuality's import UI. Automating the push via the TestQuality API is a
later step.

### What carries over to CI

`profiles.yml` (env-var driven), the dbt project, `run_qa.sh`, and
`run_results_to_junit.py` are reused **as-is**. CI replaces only the glue:
connectivity (dropped if the runner is in-cluster, or configured via CI
secrets otherwise), the scheduler (CI `schedule:` trigger), and the report
push (CI artifact / TestQuality API).
