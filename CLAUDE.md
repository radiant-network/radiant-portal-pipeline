# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
# Setup
python -m venv .venv && source .venv/bin/activate
make install-dev        # installs all deps (single pip resolve) + `airflow db init`

# Testing
make test               # static + unit + integration (non-slow); does NOT run docker tests
make test-static        # ruff check radiant/
make test-unit          # unit tests only: pytest tests/unit/
make test-integration   # fast integration: pytest -m "not slow" tests/integration
make test-integration-slow  # pytest -m slow tests/integration
make test-docker        # build-docker, then pytest tests/docker/ (drives the compose stack)

# Run a single test file or test
pytest tests/unit/path/to/test_file.py
pytest tests/unit/path/to/test_file.py::test_function_name

# Linting & formatting
make test-static        # ruff check radiant/ only
make format             # ruff format + ruff check --fix over radiant/ tests/ scripts/ecs/
```

Integration tests select their fixtures via an env var (not a make flag):
```sh
USE_DOCKER_FIXTURES=true  make test-integration   # spins up local Docker (testcontainers): MinIO + Iceberg REST catalog. CI default.
USE_DOCKER_FIXTURES=false make test-integration   # runs against the external radiant-portal-sandbox environment
```
CI (`.github/workflows/test.yml`) runs on every PR with `USE_DOCKER_FIXTURES=true`: static → unit → integration → docker. Tagged `v*` pushes build/push the two images (`build_and_push*.yml`).

## Architecture

Apache Airflow ETL pipeline importing genomic data into a clinical data model. Two storage backends: **Iceberg** (data lake, via PyIceberg + Glue/REST catalog on S3) and **StarRocks** (OLAP analytics, queried over a MySQL-protocol connection `starrocks_conn`).

### Source layout

- `radiant/dags/` — Airflow DAG definitions (orchestration only). `radiant/dags/__init__.py` is the config module (see below).
- `radiant/tasks/` — Reusable processing logic called by DAGs
  - `vcf/snv/germline/`, `vcf/snv/somatic/`, `vcf/cnv/germline/` — VCF variant extraction (cyvcf2)
  - `iceberg/` — table init, `table_accumulator.py`, partition commit
  - `starrocks/` — custom operators, partition assignment, deferrable trigger
  - `data/radiant_tables.py` — the table-name mapping resolved into SQL templates (see multi-tenancy)
  - `tracing/` — OpenTelemetry spans (exported per `otel-collector-config.yaml`)
- `radiant/dags/sql/{clinical,open_data,radiant}/` — Jinja SQL templates (each has an `init/` subdir for one-time DDL)
- `radiant/dags/operators/` — `k8s.py` and `ecs.py` task operators (selected at runtime, see deployment)
- `radiant/data_qa/` — standalone dbt project for data-quality assertions (see below); **not** part of pytest
- `tests/{unit,integration,docker}/`, `tests/resources/` — test suites + sample VCF/TSV data
- `mwaa/` — AWS MWAA + ECS deployment artifacts (separate dep builders for `airflow2/` and `airflow3/`)

### Data flow

```
VCF file → cyvcf2 parse
  → extract occurrences / variants / consequences (radiant/tasks/vcf/)
  → accumulate into Iceberg tables (radiant/tasks/iceberg/table_accumulator.py)
  → commit partition (radiant/tasks/iceberg/partition_commit.py)
  → load into StarRocks (radiant/tasks/starrocks/)
  → aggregate variant frequencies
```

### DAGs and the delta/incremental flow

- `import_radiant.py` — main scheduled import. Fetches the **sequencing-experiment delta** from StarRocks (`RadiantStarRocksOperator` + an `output_processor` that maps SQL rows → pydantic models), assigns each experiment to a partition (`SequencingExperimentPartitionAssigner`), inserts new experiments, then fires one `import_part` run per partition via `TriggerDagRunOperator`. Only new/changed experiments are processed — this is the incremental-loading mechanism (design: `design/SJRA-1187-*.md`).
- `import_part.py` — processes one partition: VCF extraction → Iceberg → StarRocks.
- `import_snv_vcf.py` — sub-DAG triggered by `import_part`, doing all SNV → Iceberg extraction. **Germline and somatic both live here on purpose**: each fans out one mapped writer per annotation task (pool `import_vcf`), and both fan into a *single* `merge_commits` → `commit_partitions`. `snv_variant` and `snv_consequence` are written by both flows, so one committer per part is what keeps their Iceberg commits from racing (design: `design/SJRA-1751-snv-vcf-ingestion-fan-out.md`).
- `import_open_data.py`, `import_brim.py` — additional sources.
- `init_iceberg_tables.py`, `init_starrocks_tables.py`, `init-qa-clinical-data.py` — one-time setup.
- `diagnostics.py` — manual ops DAG (e.g. StarRocks DNS TTL checks).
- Design rationale for major features lives in `design/SJRA-*.md`.

### Partitioning

Partitioned by `experimental_strategy` so all experiments of the same patient/family/case/sequencing-id land together. First-partition masks: WGS `0x00000000` (100 experiments/partition), WXS `0x00010000` (1000). Assignment logic in `radiant/tasks/starrocks/partition.py`. Full strategy: `docs/RADIANT.md`.

### Deployment modes and config

`radiant/dags/__init__.py` centralizes config: `NAMESPACE`, `ICEBERG_NAMESPACE` (env-overridable), `DEFAULT_ARGS`, `load_docs_md`, `get_namespace`, and the `IS_AWS` flag. **`IS_AWS` (env var) is a load-time toggle**: `import_part.py` does `if IS_AWS: from radiant.dags.operators import ecs as operators else k8s`. On AWS, `ECSEnv` pulls `AWS_ECS_*` from Airflow Variables. Three execution contexts:
- **KubernetesPodOperator** — K8s deployments (`IS_AWS=false`)
- **ECS task** — AWS ECS via custom operator (`IS_AWS=true`)
- **Local Docker Compose** — dev stack (`docker-compose.yml`): Airflow + PostgreSQL + Redis + MinIO + Polaris

`Dockerfile` = Airflow webserver/scheduler image; `Dockerfile.radiant.operator` = task-execution image with all Radiant deps.

### Multi-tenancy + SQL templating

Tables are **not** hard-coded in SQL. DAGs set `template_searchpath` to `radiant/dags/sql` and `render_template_as_native_obj=True`; the StarRocks operators inject a `mapping` dict and `tenant_code` into the Jinja context, so templates reference tables as `{{ mapping.some_table }}`. `radiant/tasks/data/radiant_tables.py` resolves that mapping from DAG-run conf + `tenant_code`: per-tenant tables route to a `{tenant}_tenant` database (`RADIANT_TENANT_DB_TEMPLATE`), shared/open-data tables stay in the base DB. `tenant_code` comes from the operator arg or `RADIANT_TENANT_CODE` in the DAG-run conf. Frequency math across tenants: `docs/frequency-calculation-multi-tenant.md`.

### StarRocks operator patterns

`radiant/tasks/starrocks/operator.py` — long-running loads run as StarRocks **async tasks** (`SUBMIT TASK`), then a **deferrable** `StarRocksTaskCompleteTrigger` (`trigger.py`) polls for completion without holding a worker slot. `SubmitTaskOptions` controls timeout/poll/spill. Key operators: `RadiantStarRocksOperator` (query + optional async submit + `output_processor`), `RadiantStarRocksPartitionSwapOperator` / `SwapPartition` (atomic partition replace for idempotent reloads), `RadiantLoadExomiserOperator`. Inserts serialize via `max_active_tis_per_dagrun=1` on the mapped operators plus the serial `>>` chains in `import_part.py` — note `STARROCKS_INSERT_POOL` is declared in `operator.py` but never referenced, so no pool is actually involved.

### Data QA (dbt) — separate from pytest

`radiant/data_qa/` is a dbt project (no models) asserting data quality against StarRocks. Run via `radiant/data_qa/scripts/run_qa.sh`: `run_qa.py` → `run_results_to_junit.py` → JUnit XML (test failures encoded in XML, exit non-zero only on connection/mechanism failure). Custom generic/singular tests live in `tests/*.sql`, reusable macros in `macros/`. **Two dbt sources, because tables live in two kinds of database**: `radiant` (shared, `SR_SCHEMA`) and `tenant_db` (per-tenant `{tenant}_tenant`, `SR_TENANT_SCHEMA`) — membership follows `STARROCKS_RADIANT_PER_TENANT_MAPPING` in `radiant/tasks/data/radiant_tables.py`. dbt resolves `source.schema` at parse time, so `run_qa.py` invokes dbt 1+N times (`--exclude source:tenant_db` once, then `--select source:tenant_db` per tenant) and merges the artifacts into the single `run_results.json`/`junit.xml` pair the DAG uploads; each result is stamped with its tenant. The tenant list arrives as the `TENANTS` env var (JSON, with schemas already resolved Airflow-side). The **"Values Contained In Dictionary"** tests exist to catch new upstream enum values the portal can't yet handle; their accepted-value lists (in `macros/dictionaries.sql`) mirror the portal's `backend/internal/repository/facets.go` **and** frontend i18n — keep them in sync when adding values. See `radiant/data_qa/README.md`.

## Linting conventions

Ruff (`.ruff.toml`): line length 119, Python 3.12, double quotes, Google-style docstrings. Rules: E, F, UP, B, SIM, I. `make test-static` only checks `radiant/`; `make format` also fixes `tests/` and `scripts/ecs/`.
