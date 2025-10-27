.PHONY: all

build-docker:
	docker build -t radiant-airflow:latest .
	docker build -t ghcr.io/radiant-network/radiant-airflow-task-operator:latest -f Dockerfile.radiant.operator .

install:
	pip install -r requirements-operator.txt

install-dev: install
	pip install -r requirements-dev.txt

	# Required for standalone unit tests
	airflow db init

test-docker: build-docker
	pytest tests/docker/

test-unit:
	pytest tests/unit/

test-integration:
	pytest -m "not slow" tests/integration

test-integration-slow:
	pytest -m slow tests/integration

test-static:
	ruff check radiant/

test: test-static test-unit test-integration

format:
	ruff format radiant/ tests/ scripts/ecs/
	ruff check --fix radiant/ tests/ scripts/ecs/
