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
| Values Contained In Dictionary | scalar: `accepted_values` (built-in); array: `accepted_values_in_array` (custom) |
| Should Be Within Range         | `should_be_within_range` (dynamic, custom)                    |
| Cross-Field / Custom Invariant | Singular tests in `tests/*.sql`                               |

Three **dynamic** tests introspect the table's columns at compile time and
emit one check per column: `should_not_contain_only_null` and
`should_not_contain_same_value` sweep all columns minus an `except` list;
`should_be_within_range` sweeps only columns matching a `like` substring
(default `_pf_`). New matching columns are picked up automatically; only the
`except` / `like` config needs maintenance.

For the built-in dbt tests (`not_null`, `unique`), the YAML keeps standard
dbt syntax — only the displayed name is overridden via `name:` so that
TestQuality / JUnit reports show consistent vocabulary
(`should_not_contain_null_<col>`, `should_be_unique_<col>`).

### Purpose of `Values Contained In Dictionary` tests

These tests exist to **detect new upstream values that the portal does not
yet handle**. "The portal" hardcodes behaviour in two places:

1. **Backend Go facets** — `backend/internal/repository/facets.go`
   (`NewFacetsRepository`). The closed list of values the API surfaces
   for a column when the frontend calls it with `withDictionary: true`.
   A value not in this list **cannot be selected as a filter** even if
   it appears in result rows.
2. **Frontend i18n** — `frontend/translations/common/*.json`. The closed
   list of values that have hardcoded translations, badge colours,
   abbreviations, icons, sort order, etc. A value not in this list
   **renders as raw text in the UI**.

A new upstream value not present in **either** source is fully unhandled.
A value present in only one is a portal-internal inconsistency (out of
scope for data-qa, but worth flagging).

Each test's accepted-values list mirrors the relevant source(s) — the
**union** of backend facets and frontend i18n for scalar columns. The lists
themselves live in a single place — `macros/dictionaries.sql` — one
`dict_<name>()` macro per dictionary, so a value shared across tables (e.g.
`consequences`, `vep_impact`, `chromosome`, `zygosity`) is declared once. A
source YAML references it with `values: "{{ dict_<name>() }}"` (quoted for
YAML validity; dbt renders the macro to the list at compile time — do **not**
add `| tojson`, which breaks the `return()`-based macros). The
`mirrors facets.go + i18n — keep in sync` comment lives next to each macro, so
that file is the one place to update when an upstream value changes.

Rule of thumb:

- **Keep the test** if the column drives portal behaviour through a
  closed set of values in either backend facets or frontend i18n (e.g.
  `consequences`, `vep_impact`, `variant_class`, `clinvar_interpretation`,
  `chromosome` — all hardcoded somewhere).
- **Drop the test** if the column is free-form / informational only and
  the portal renders the raw value as-is everywhere (no backend facet,
  no frontend i18n). Maintaining a dictionary the portal doesn't depend
  on is busywork that will rot.

Scalar columns use dbt's built-in `accepted_values`. Array columns use the
custom `accepted_values_in_array` generic test (`macros/`), which `array_filter`s
each array down to the offending elements *before* unnesting (so valid rows
emit nothing instead of exploding) and flags any element not in `values`. Both are declared in the
source YAML with a `name:` (append the Jira id when a ticket tracks a known
gap, e.g. `..._SJRA1552`) and a `values:` list.

Values are tested **raw / case-sensitive** — no normalization. For array
columns the dictionary therefore mirrors backend `facets.go` in its data
case (e.g. `mature_miRNA_variant`, `TFBS_ablation`); frontend i18n short
forms are sanitized lookup keys that never appear in raw data, so they're
out of scope. Singular tests under `tests/` are now reserved for cross-field
invariants that don't fit a generic test (e.g. `no_star_alternate`).

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
`.env.example` to `.env` and fill in the values, then load it into your shell:

```bash
set -a && source .env && set +a   # export every var from .env
```

Or export them directly in your shell:

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

# Scope to one table (list available tables with the command below)
dbt test --select source:radiant.<table>
dbt list --resource-type source     # shows every source:radiant.<table>
```

dbt writes detailed results to `target/run_results.json`. Each failing test
also writes its failing rows to `target/compiled/.../...sql` — useful to
investigate.

## Layout

```
data-qa/
├── macros/    # custom generic tests + dictionaries.sql
├── sources/   # source + generic-test declarations, one YAML per table
├── tests/     # singular tests (cross-field invariants), one SQL per check
├── scripts/   # dbt test runner + JUnit conversion (CI-ready)
├── reports/   # generated JUnit XML (gitignored) for TestQuality
└── models/    # empty by design
```

Root also holds the usual dbt config (`dbt_project.yml`, `profiles.yml`,
`packages.yml`, `requirements.txt`).

## Adding tests for another table

1. Add a `sources/<table_name>.yml` file (one per table, named after it).
   Each declares the shared `radiant` source with a single table — dbt merges
   tables across files as long as no `(source, table)` pair is duplicated.
2. List its columns with the relevant generic tests, and/or attach
   `should_not_contain_only_null` / `should_not_contain_same_value` at the
   table level with an appropriate `except` list. For an `accepted_values` /
   `accepted_values_in_array` test, reference the shared list via
   `values: "{{ dict_<name>() }}"`; add a new macro to
   `macros/dictionaries.sql` if the dictionary doesn't exist yet.
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
