# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
# Setup
python -m venv .venv && source .venv/bin/activate
make install-dev        # pip install -r requirements.txt -r requirements-dev.txt (one resolve, so both sets of constraints apply)

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
USE_DOCKER_FIXTURES=true  make test-integration   # spins up local Docker (testcontainers): RustFS (S3) + Iceberg REST catalog. CI default.
USE_DOCKER_FIXTURES=false make test-integration   # runs against the external radiant-portal-sandbox environment
```
CI (`.github/workflows/test.yml`) runs on every PR with `USE_DOCKER_FIXTURES=true`: static → unit → integration → docker. Tagged `v*` pushes build/push the two images (`build_and_push*.yml`).

**Two Airflow versions.** The venv and the tests pin `apache-airflow==2.10.5` (`requirements-dev.txt`, `requirements-airflow.txt`); the deployed Airflow is **3.2.1** (`mwaa/` keeps separate `airflow2/` and `airflow3/` builds). Check scheduler behaviour (pools, deferred tasks, run-id rules, task-id changes in active runs) against the 3.2.1 source, not against the venv.

**Integration test data.** `radiant/dags/sql/clinical/seeds.sql` feeds every clinical integration test, including the whole-delta row count in `tests/integration/dags/sql/test_sequencing_experiment_select_delta.py` (18 rows): a seeded document that the staging view admits moves that count. Shapes the seeds do not have are inserted by test-local fixtures on ids far above the seeded range — 90xx–93xx are taken by `test_case_resolution.py`, 95xx by `test_somatic_case_resolution.py` — and a fixture that inserts rows must delete them afterwards, or it changes the results of other tests in the same session.

### Local Airflow stack

`make build-docker`, then `docker compose up`. UI at `http://localhost:8080` (`airflow` / `airflow`). Create the MySQL connection `starrocks_conn` (Admin → Connections; e.g. host `host.docker.internal`, login `root`). In deployed environments, give it socket timeouts in `Extra` (`{"connect_timeout": 10, "read_timeout": 60, "write_timeout": 60}`): `StarRocksTaskCompleteTrigger` bounds each poll, but a timed-out poll's worker thread stays blocked in `recv()` on a half-open connection until a driver-level `read_timeout` releases it. Check the extras against the image's `apache-airflow-providers-mysql` version: the hook forwards only whitelisted extras, and an unknown key breaks `get_conn()`.

## Architecture

Apache Airflow ETL pipeline importing genomic data into a clinical data model. Two storage backends: **Iceberg** (data lake, via PyIceberg + Glue/REST catalog on S3) and **StarRocks** (OLAP analytics, queried over a MySQL-protocol connection `starrocks_conn`).

### Source layout

- `radiant/dags/` — Airflow DAG definitions (orchestration only). `radiant/dags/__init__.py` is the config module (see below). Each DAG's UI doc is `radiant/dags/docs/<dag>.md`, loaded via `load_docs_md(...)` into `doc_md` — update it with the DAG.
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
- `nextflow_snv_postprocessing.py` / `nextflow_quality_control.py` — thin launchers: one Nextflow driver pod on k8s each. `nextflow_snv_postprocessing_cases.py` / `nextflow_cnv_postprocessing_cases.py` / `nextflow_quality_control_cases.py` — the automations around them (no schedule of their own, see `case_scheduler.py`): discover pending cases in the clinical model (`sql/clinical/pending_*_select.sql`), build inputs on S3, trigger the launcher, register the outputs on the cases through the portal batch PATCH (`radiant/tasks/nextflow/`, QC-specific parts in `radiant/tasks/nextflow/qc/`). Designs: `design/SJRA-1698-*.md`, `design/SJRA-1879-*.md`. Somatic SNV annotation (RAD-23) has its own pair: `nextflow_somatic_snv_annotation.py` (launcher: the SNV pipeline, image and `nextflow-snv-cfg` with `nextflow-snv-somatic-params`) and `nextflow_somatic_snv_annotation_cases.py` (unit of work: the calling task, per case; rules in `radiant/tasks/nextflow/somatic/`, query `sql/clinical/pending_somatic_annotation_select.sql`).
- **Nextflow concurrency.** All drivers share one FSx file system, and the launch dir, `workDir`, outputs and cleanup are all keyed by `RUN_TAG`. The case scheduler gives every cases DAG the same run id, so a cases DAG whose launcher could see the same tag must prefix it (the somatic one uses `somatic-`). The SNV, CNV and somatic launchers run one driver at a time (`max_active_runs=1`; the QC launcher allows 5): a germline WGS run leaves about 200–280 GB of scratch on the 1.2 TiB file system. Limit drivers per launcher DAG, not per task: the driver is deferred for almost all of its run, and Airflow counts deferred tasks neither for `max_active_tis_per_dag` (RUNNING and QUEUED only) nor for a pool without `include_deferred`.
- `case_scheduler.py` ("Radiant - Case Scheduler") — the single `@daily` entry point for case processing and the only place case statuses change. Discovers the `submitted` cases with work waiting (each cases DAG's own `pending_*` query + `select_cases`, plus what the import still has to load: the delta view and the staging rows `import_part` hasn't loaded, `sql/radiant/import_pending_case_select.sql`), moves them to `processing` (one portal PATCH per tenant), triggers the four cases DAGs and, after germline SNV post-processing and somatic SNV annotation, the scheduled import (all `all_done`, so a portal outage never blocks data), then re-runs the same discovery and moves to `in_progress` every `processing` case that no pipeline has work for any more and that has imported SNV/CNV VCFs. Discovery counts only what the cases DAGs would actually run, QC's S3 metrics probe included, or a case would sit in `processing` for ever. It is also where the labs are emailed: `evaluate_cases` is a mapped task group per tenant (`set_in_progress` → `create_case_group` `results-<run tag>` → `notify_labs`), so the manifest holds every pipeline's results; every case the run finished is emailed, the ones moved to `in_progress` and the ones already past it (status untouched, `processed_case_ids`, from `discover_cases`'s `cases_with_work` XCom); the cases DAGs don't email. Tenants are filtered by a block list, `excluded_tenants` (default `$RADIANT_ETL_EXCLUDED_TENANTS`, from `radiant/dags/__init__.py`, shared with the four cases DAGs): discovery reports them as `tenant_excluded`, the cases DAGs get the same list in their trigger `conf`, and their statuses are left alone. A non-excluded tenant without the grant is skipped on its 403. Rules in `radiant/tasks/nextflow/case_status.py`, query in `sql/clinical/case_status_select.sql`.
- `import_open_data.py`, `import_brim.py` — additional sources. `import_open_data` reads each open-data source either from the OpenDataLake catalog under its contract name or from the Radiant Iceberg catalog under its pre-contract name (`RADIANT_OPEN_DATA_USE_LEGACY_TABLES`), refreshes the StarRocks metadata cache first, and skips its cytoband broker load unless the caller passes its path. The `clinvar_rcv_summary` task group is one serial chain into the raw RCV table — an insert from the OpenDataLake `clinvar_rcv_v1` contract table (skipped when held back), then the broker load if `raw_rcv_filepaths` is passed, then the summary projection. `clinvar_rcv` is the one contract source with no pre-contract Radiant table: held back it resolves to no relation at all, leaving the broker load as the only way to fill it. It also triggers `import_cosmic_gene_set.py` when given `cosmic_gene_set_filepath`: that DAG broker-loads the COSMIC Cancer Gene Census TSV into `cosmic_gene_set` (positional columns, transforms in the load's `SET` clause) and rebuilds `cosmic_gene_panel` from it — COSMIC has no OpenDataLake contract and is no longer an Iceberg source. Likewise `cosmic_mutation_set_filepath` triggers `import_cosmic_mutation_set.py`: a normalize task in the radiant-operator image (k8s or ECS; the image carries `bcftools`) rebuilds COSMIC's anchor-less indels as VCF records against the GRCh38 FASTA (`RADIANT_REFERENCE_FASTA_S3_URI`), left-aligns them, and writes a keyed TSV that is broker-loaded into `raw_cosmic_mutation_set` and deduplicated per locus into `cosmic_mutation_set` (`radiant/tasks/open_data/cosmic_mutation_set.py`); the rows coordinates cannot place (no GRCh38 position, strand-flipped liftover) are kept in the staging table with NULL keys and feed `cosmic_mutation_set_hgvs`, keyed on (Ensembl transcript without version, `c.` change). `snv_staging_variant_insert.sql` / `_reannotate.sql` denormalise both into the four `cmc_*` columns of the variant tables: the locus row when there is one, otherwise the HGVS row matching the picked `transcript_id` + `dna_change` (broadcast join), never a mix of the two. A COSMIC refresh is invisible to `reannotate_open_data`'s gate, so it needs a forced re-annotation.
- `reannotate_open_data.py` — weekly (Sat 00:00) open-data refresh: triggers `import_open_data`, then re-annotates what derives from it. Design: `design/SJRA-1811-opendatalake-integration.md`.
- **Import mutex**: `import_part` and `reannotate_open_data` share one S3 lock (`radiant/tasks/locking.py`, `_locks/import_mutex` in `RADIANT_LOCK_S3_BUCKET`, stale after 6 h) so they never write the same tables at once. `toolbox`'s `check-lock` command reports/clears it.
- `init_iceberg_tables.py`, `init_starrocks_tables.py`, `init-qa-clinical-data.py` — one-time setup.
- `diagnostics.py` — manual ops DAG (e.g. StarRocks DNS TTL checks).
- `toolbox.py` — manual: one maintenance command per run (`create-tenant`, `create-user`, `update-user`, `refresh-tenants` run the radiant-portal backend toolbox image on ECS/K8s; `check-lock` runs in-DAG).
- `data_integrity_starrocks.py` — runs the `data_qa` dbt suite in a container (ECS/K8s), reads results back from S3, optionally pushes JUnit to TestQuality (`testquality_conn`).
- `notify_cases.py` — manual re-send of a case group's lab email (`POST /{tenant}/case_groups/{name}/notify`); every run sends, no dedup.
- `import_igv_tracks.py` — manual, once per env: builds the public IGV reference tracks (+ tabix index) under one S3 prefix. Files only, no StarRocks/Iceberg.
- `import_tenant_gene_panel.py` — manual: reads one tenant gene panel `.tsv` from S3 and attaches it unchanged to one portal `PUT /{tenant}/gene_panels` (`radiant/tasks/gene_panel/upload.py`, client in `radiant/tasks/nextflow/portal.py`, connection `radiant_api_conn`). The portal parses the file (columns `symbol` + comma-separated `panels`) and sets the genes of all the tenant's panels from it (catalog panels keep their row, uploaded panels missing from the file are removed); the service account needs `can_manage_analysis_catalog`, not `ingest_data`. A 4xx fails with no retry, a 5xx is retried (the upload is idempotent). Design: radiant-hub `docs/features/RAD-10-*.md`.
- Design rationale for major features lives in `design/SJRA-*.md`.
- DAG docs (`radiant/dags/docs/*.md`, loaded by `load_docs_md` into `doc_md`): the Airflow docs panel of the deployment renders inline code as a broken block and does not render mermaid. Write identifiers in **bold**, use tables, lists and blockquotes, and no code fences. Escape double underscores (`\_\_`) and a bare `*`.

### Partitioning

Partitioned by `experimental_strategy` so all experiments of the same patient/family/case/sequencing-id land together. First-partition masks: WGS `0x00000000` (100 experiments/partition), WXS `0x00010000` (1000). Assignment logic in `radiant/tasks/starrocks/partition.py`. Full strategy: `docs/RADIANT.md`.

### Deployment modes and config

`radiant/dags/__init__.py` centralizes config: `NAMESPACE`, `ICEBERG_NAMESPACE` (env-overridable), `DEFAULT_ARGS`, `load_docs_md`, `get_namespace`, and the `IS_AWS` flag. **`IS_AWS` (env var) is a load-time toggle**: `import_part.py` does `if IS_AWS: from radiant.dags.operators import ecs as operators else k8s`. On AWS, `ECSEnv` pulls `AWS_ECS_CLUSTER`/`_SUBNETS`/`_SECURITY_GROUPS` from Airflow Variables. `RADIANT_S3_WORKSPACE` (env var, every deployment) is the bucket where Airflow stores payloads too large to pass inline to a task (`s3_store_content`), e.g. the merged partitions `merge_commits` hands to the commit task. Three execution contexts:
- **KubernetesPodOperator** — K8s deployments (`IS_AWS=false`)
- **ECS task** — AWS ECS via custom operator (`IS_AWS=true`)
- **Local Docker Compose** — dev stack (`docker-compose.yml`): Airflow + PostgreSQL + Redis + RustFS (S3, keeps the `radiant-minio` host name) + Polaris

`Dockerfile` = Airflow webserver/scheduler image; `Dockerfile.radiant.operator` = task-execution image with all Radiant deps.

### Multi-tenancy + SQL templating

Tables are **not** hard-coded in SQL. DAGs set `template_searchpath` to `radiant/dags/sql` and `render_template_as_native_obj=True`; the StarRocks operators inject a `mapping` dict and `tenant_code` into the Jinja context, so templates reference tables as `{{ mapping.some_table }}`. `radiant/tasks/data/radiant_tables.py` resolves that mapping from DAG-run conf + `tenant_code`: per-tenant tables route to a `{tenant}_tenant` database (`RADIANT_TENANT_DB_TEMPLATE`), shared/open-data tables stay in the base DB. `tenant_code` comes from the operator arg or `RADIANT_TENANT_CODE` in the DAG-run conf. Frequency math across tenants: `docs/frequency-calculation-multi-tenant.md`.

### StarRocks operator patterns

`radiant/tasks/starrocks/operator.py` — long-running loads run as StarRocks **async tasks** (`SUBMIT TASK`), then a **deferrable** `StarRocksTaskCompleteTrigger` (`trigger.py`) polls for completion without holding a worker slot. `SubmitTaskOptions` controls timeout/poll/spill. Key operators: `RadiantStarRocksOperator` (query + optional async submit + `output_processor`), `RadiantStarRocksPartitionSwapOperator` / `SwapPartition` (atomic partition replace for idempotent reloads), `RadiantLoadExomiserOperator`. Inserts serialize via `max_active_tis_per_dagrun=1` on the mapped operators plus the serial `>>` chains in `import_part.py` — note `STARROCKS_INSERT_POOL` is declared in `operator.py` but never referenced, so no pool is actually involved.

### Data QA (dbt) — separate from pytest

`radiant/data_qa/` is a dbt project (no models) asserting data quality against StarRocks. Run via `radiant/data_qa/scripts/run_qa.sh`: `run_qa.py` → `run_results_to_junit.py` → JUnit XML (test failures encoded in XML, exit non-zero only on connection/mechanism failure). Custom generic/singular tests live in `tests/*.sql`, reusable macros in `macros/`. **Two dbt sources, because tables live in two kinds of database**: `radiant` (shared, `SR_SCHEMA`) and `tenant_db` (per-tenant `{tenant}_tenant`, `SR_TENANT_SCHEMA`) — membership follows `STARROCKS_RADIANT_PER_TENANT_MAPPING` in `radiant/tasks/data/radiant_tables.py`. dbt resolves `source.schema` at parse time, so `run_qa.py` invokes dbt 1+N times (`--exclude source:tenant_db` once, then `--select source:tenant_db` per tenant) and merges the artifacts into the single `run_results.json`/`junit.xml` pair the DAG uploads; each result is stamped with its tenant. The tenant list arrives as the `TENANTS` env var (JSON, with schemas already resolved Airflow-side). The **"Values Contained In Dictionary"** tests exist to catch new upstream enum values the portal can't yet handle; their accepted-value lists (in `macros/dictionaries.sql`) mirror the portal's `backend/internal/repository/facets.go` **and** frontend i18n — keep them in sync when adding values. See `radiant/data_qa/README.md`.

## Linting conventions

Ruff (`.ruff.toml`): line length 119, Python 3.12, double quotes, Google-style docstrings. Rules: E, F, UP, B, SIM, I. `make test-static` only checks `radiant/`; `make format` also fixes `tests/` and `scripts/ecs/`.
