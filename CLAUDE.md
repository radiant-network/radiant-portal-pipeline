# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
# Setup
python -m venv .venv && source .venv/bin/activate
make install-dev        # installs all deps + initializes Airflow DB

# Testing
make test               # static checks + unit + integration (non-slow)
make test-static        # ruff check only
make test-unit          # unit tests only: pytest tests/unit/
make test-integration   # fast integration: pytest -m "not slow" tests/integration
make test-integration-slow  # pytest -m slow tests/integration
make test-docker        # builds docker images then runs pytest tests/docker/

# Run a single test file or test
pytest tests/unit/path/to/test_file.py
pytest tests/unit/path/to/test_file.py::test_function_name

# Linting & formatting
make test-static        # ruff check radiant/
make format             # ruff format + ruff check --fix
```

Integration tests use Docker fixtures by default:
```sh
USE_DOCKER_FIXTURES=true make test-integration    # local Docker (default in CI)
USE_DOCKER_FIXTURES=false make test-integration   # against sandbox environment
```

## Architecture

This is an Apache Airflow ETL pipeline for importing genomic data into a clinical data model. The two main storage backends are **Iceberg** (data lake) and **StarRocks** (OLAP analytics).

### Source layout

- `radiant/dags/` — Airflow DAG definitions (orchestration only)
- `radiant/tasks/` — Reusable processing logic called by DAGs
  - `vcf/snv/germline/`, `vcf/snv/somatic/`, `vcf/cnv/germline/` — VCF variant extraction
  - `iceberg/` — Iceberg table initialization, accumulation, and partition commit
  - `starrocks/` — StarRocks operator, partition logic, and triggers
  - `data/` — Table schema definitions shared across tasks
- `radiant/dags/sql/` — SQL templates (clinical, open_data, radiant subdirs)
- `radiant/dags/operators/` — Custom Kubernetes and ECS task operators
- `scripts/ecs/` — ECS-specific entrypoint scripts
- `tests/unit/`, `tests/integration/`, `tests/docker/` — Test suites
- `tests/resources/` — Test data: sample VCF files, TSV partitions

### Data flow

```
VCF file
  → parse with cyvcf2
  → extract occurrences / variants / consequences (radiant/tasks/vcf/)
  → accumulate into Iceberg tables (radiant/tasks/iceberg/table_accumulator.py)
  → commit partition (radiant/tasks/iceberg/)
  → load into StarRocks (radiant/tasks/starrocks/)
  → aggregate variant frequencies
```

### Partitioning

Genomic data is partitioned by `experimental_strategy` to keep partition sizes manageable. Partitions are assigned so that all experiments belonging to the same patient, family, case, or sequencing ID land in the same partition. See [docs/RADIANT.md](docs/RADIANT.md) for the full strategy.

| Strategy | First partition mask | Experiments per partition |
|----------|---------------------|--------------------------|
| WGS | `0x00000000` | 100 |
| WXS | `0x00010000` | 1000 |

### DAGs

- `import_radiant.py` — Main scheduled import; assigns partitions then triggers `import_part` per partition
- `import_part.py` — Processes one partition: VCF extraction → Iceberg → StarRocks
- `import_germline_snv_vcf.py` — Germline SNV VCF import
- `import_open_data.py`, `import_brim.py` — Additional data sources
- `init_iceberg_tables.py`, `init_starrocks_tables.py` — One-time table setup

### Deployment modes

Tasks can run in three execution contexts (selected per deployment):
- **KubernetesPodOperator** — K8s-based deployments
- **ECS task** — AWS ECS via custom operator (`radiant/dags/operators/`)
- **Local Docker Compose** — Development stack (`docker-compose.yml`): Airflow + PostgreSQL + Redis + MinIO + Polaris

The `Dockerfile` is the Airflow webserver/scheduler image; `Dockerfile.radiant.operator` is the task execution image with all Radiant dependencies.

### Linting conventions

Ruff is configured in `.ruff.toml`: line length 119, Python 3.12, double quotes, Google-style docstrings. Rules: E, F, UP, B, SIM, I (isort).
