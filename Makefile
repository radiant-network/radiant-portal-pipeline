.PHONY: all

build-docker:
	docker build -t radiant-airflow:latest .
	docker build -t ghcr.io/radiant-network/radiant-airflow-task-operator:latest -f Dockerfile.radiant.operator .

install:
	pip install -r requirements.txt

install-dev:
	# Installing requirements in a single command to ensure constraints from both requirement files are resolved together
	pip install -r requirements.txt -r requirements-dev.txt

	# Required for standalone unit tests
	airflow db migrate

test-docker: build-docker
	pytest tests/docker/

test-unit:
	pytest tests/unit/

test-integration:
	pytest -m "not slow" tests/integration

test-integration-slow:
	pytest -m slow tests/integration

test-static:
	ruff check radiant/ scripts/upgrade_guide/

test: test-static test-unit test-integration

# Rust SNV engine, germline and somatic (POC, rust/radiant_vcf/README.md). Needs cargo and maturin (pip install maturin).
rust-develop:
	maturin develop --release -m rust/radiant_vcf/Cargo.toml

rust-test: rust-develop
	cargo fmt --manifest-path rust/radiant_vcf/Cargo.toml --check
	cargo clippy --manifest-path rust/radiant_vcf/Cargo.toml --all-targets -- -D warnings
	cargo test --manifest-path rust/radiant_vcf/Cargo.toml
	pytest tests/unit/vcf/test_rust_engine.py tests/unit/vcf/germline/test_rust_engine_parity.py \
		tests/unit/vcf/somatic/test_rust_engine_parity.py

rust-bench: rust-develop
	python rust/radiant_vcf/bench.py --flavour germline --records 100000
	python rust/radiant_vcf/bench.py --flavour somatic --records 200000

format:
	ruff format radiant/ tests/ scripts/ecs/ scripts/upgrade_guide/
	ruff check --fix radiant/ tests/ scripts/ecs/ scripts/upgrade_guide/

# The upgrade guide between two releases, from the notes in upgrade-notes/ (README, "Upgrade guide").
# TO is a vX.Y.Z tag, or a branch (main) for the notes not released yet. Needs the tags: git fetch --tags
upgrade-guide:
	@test -n "$(FROM)" -a -n "$(TO)" || { echo "usage: make upgrade-guide FROM=<vA> TO=<vB>"; exit 2; }
	@python -m scripts.upgrade_guide guide --from "$(FROM)" --to "$(TO)"
