# Radiant Data QA

dbt project for running data-quality assertions against the Radiant
StarRocks database, without producing any models. Generic and singular
tests assert null-safety, uniqueness, accepted values, and cross-field
invariants on tables.

## Multi-tenancy

Radiant's tables live in **two** kinds of database, so this project declares
**two dbt sources** — which one a table belongs to is decided by
`STARROCKS_RADIANT_PER_TENANT_MAPPING` in
`radiant/tasks/data/radiant_tables.py`, the authority for the split:

| source | schema env var | holds |
| ------ | -------------- | ----- |
| `radiant` | `SR_SCHEMA` | open data, `snv__consequence*`, staging — one shared copy |
| `tenant_db` | `SR_TENANT_SCHEMA` | `germline__snv__occurrence`, `somatic__snv__occurrence`, `germline__cnv__occurrence`, `somatic__cnv__occurrence`, `snv__variant`, `exomiser` — one copy per tenant, in `{tenant}_tenant` |

dbt resolves `source.schema` at **parse time**, one value per invocation, so a
single `dbt test` cannot fan a source across N tenant databases. The suite
therefore runs **1 + N passes**:

```
pass 0  shared    dbt test --exclude source:tenant_db
pass i  <tenant>  dbt test --select  source:tenant_db   (SR_TENANT_SCHEMA=<tenant>_tenant)
```

`scripts/run_qa.py` drives that loop and merges the passes into the single
`target/run_results.json` + `reports/junit.xml` pair the pipeline expects. The
split is what buys the **selection**: without it, the ~120 shared assertions
would re-run once per tenant.

The two selectors are complementary and disjoint (dbt's default *eager*
indirect selection), so every test node runs in exactly one pass. A test that
joins a per-tenant table to a shared one — e.g.
`snv_consequence_filter_partitioned__validate_completeness_vs_snv_consequence`
— is pulled out of the shared pass and into the tenant pass, which is where it
belongs: it is a per-tenant assertion.

**Which source does a new test belong to?** Whichever source its table is
declared under. A test that touches *both* needs no special handling — it lands
in the tenant pass automatically. But mind the direction: an assertion from a
shared table *into* a per-tenant one is generally invalid, because each tenant's
tables hold only that tenant's rows (see SJRA-1754). The reverse — per-tenant
into shared — is fine, and becomes a genuine cross-database join.

Verify the partition after touching any source declaration:

```bash
dbt ls --resource-type test --select source:tenant_db   # per-tenant assertions
dbt ls --resource-type test --exclude source:tenant_db  # shared assertions
```

## Test Categories

| Category                       | dbt implementation                              |
| ------------------------------ | ----------------------------------------------- |
| Should Not Contain Null        | `tests: [not_null]` (built-in, displayed via custom `name:`)  |
| Should Not Contain Only Null   | `should_not_contain_only_null` (dynamic, custom)              |
| Should Not Contain Same Value  | `should_not_contain_same_value` (dynamic, custom)             |
| Should Be Unique               | `tests: [unique]` (built-in, displayed via custom `name:`)    |
| Should Not Be Empty            | `should_not_be_empty` (custom generic, table-level)           |
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
scope for data_qa, but worth flagging).

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
gap, e.g. `..._SJRA-1552`) and a `values:` list.

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
cd data_qa
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

Two more vars drive the per-tenant passes (see [Multi-tenancy](#multi-tenancy)):

- `SR_TENANT_SCHEMA` — the tenant database a single `dbt` invocation reads.
  Defaults to `SR_SCHEMA`, which is only useful for parsing: the per-tenant
  tables do not exist in the base database.
- `TENANTS` — JSON consumed by `scripts/run_qa.py`, one extra pass per entry:

  ```json
  [{"code": "chusj", "schema": "chusj_tenant"}, {"code": "chop", "schema": "chop_tenant"}]
  ```

  The **schema is passed in already resolved**, never rebuilt from the code:
  the database name comes from `RADIANT_TENANT_DB_TEMPLATE`, which is
  configurable, and Airflow owns that resolution.

## Run

```bash
# Sanity check connection
dbt debug

# Run the shared (open data / base) assertions only
dbt test --exclude source:tenant_db

# Run the per-tenant assertions against one tenant
SR_TENANT_SCHEMA=chusj_tenant dbt test --select source:tenant_db

# Scope to one table (list available tables with the command below)
dbt test --select source:radiant.<table>          # shared table
dbt test --select source:tenant_db.<table>   # per-tenant table
dbt list --resource-type source                   # shows every source:<source>.<table>
```

A bare `dbt test` runs both sources in one invocation, against a single schema
— which is wrong for the per-tenant tables unless every tenant happens to live
in `SR_TENANT_SCHEMA`. Use `./scripts/run_qa.sh` instead, which does the full
1 + N loop:

```bash
# shared pass only
./scripts/run_qa.sh

# shared pass + one tenant
TENANTS='[{"code":"chusj","schema":"chusj_tenant"}]' ./scripts/run_qa.sh
```

dbt writes detailed results to `target/run_results.json`. Each failing test
also writes its failing rows to `target/compiled/.../...sql` — useful to
investigate.

## Layout

```
data_qa/
├── macros/    # custom generic tests + dictionaries.sql
├── sources/   # source + generic-test declarations, one YAML per table
├── tests/     # singular tests (cross-field invariants), one SQL per check
├── scripts/   # 1+N dbt runner + JUnit conversion (CI-ready)
├── reports/   # generated JUnit XML (gitignored) for TestQuality
└── models/    # empty by design
```

Root also holds the usual dbt config (`dbt_project.yml`, `profiles.yml`,
`packages.yml`, `requirements.txt`).

## Adding tests for another table

1. Add a `sources/<table_name>.yml` file (one per table, named after it).
   Each declares a single table under either the `radiant` or the
   `tenant_db` source — see [Multi-tenancy](#multi-tenancy) for which one,
   and copy the header from an existing file of that kind. dbt merges tables
   across files as long as no `(source, table)` pair is duplicated.
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
  A result carrying a `tenant` is named `<test>[tenant=<code>]`, the standard
  parametrized-test convention: a per-tenant assertion keeps the same dbt
  `unique_id` in every pass (the schema only ever appears in `compiled_code`),
  so without the suffix the cases would collide and "which tenant failed" would
  be lost.
- **`scripts/run_qa.py`** — runs dbt once per pass (see
  [Multi-tenancy](#multi-tenancy)), stamps the tenant onto every result, and
  merges the passes into one `run_results.json` before converting. A failing
  tenant does **not** abort the loop — tenant B is still worth testing when
  tenant A's database is broken — but a pass that produced no artifact at all
  is recorded as an `error` result rather than silently vanishing. Per-pass raw
  artifacts are kept under `target/run_results/<label>.json` for debugging.
- **`scripts/run_qa.sh`** — the reusable core: environment setup (venv, `.env`,
  profiles dir) then `run_qa.py`. Assumes StarRocks is already reachable (VPN,
  tunnel, or in-cluster runner). **Reused verbatim in CI**, and by the Airflow
  pipeline via `scripts/dbt/entrypoint.py`.

### Generating the report

With connectivity to StarRocks established (see Configuration above):

```bash
./scripts/run_qa.sh        # 1+N dbt passes + writes reports/junit.xml
```

### Import into TestQuality

`reports/junit.xml` is a standard JUnit XML file — upload it through
TestQuality's import UI. Automating the push via the TestQuality API is a
later step.

### What carries over to CI

`profiles.yml` (env-var driven), the dbt project, `run_qa.sh`, `run_qa.py` and
`run_results_to_junit.py` are reused **as-is**. CI replaces only the glue:
connectivity (dropped if the runner is in-cluster, or configured via CI
secrets otherwise), the scheduler (CI `schedule:` trigger), and the report
push (CI artifact / TestQuality API).


## Development
### Code formatting
  
  The Python files in this folder are subject to the repository's `ruff` formatting rules
  (enforced in CI / unit tests).

  The dbt virtualenv does **not** include `ruff`, so install it first — pinned to the same version
  as `requirements-dev.txt` to avoid reformatting churn:

  ```bash
  pip install ruff==0.11.4

  # format (from the root of the repo)
  ruff format radiant/data_qa/ && ruff check --fix radiant/data_qa/
  ```
