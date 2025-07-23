.PHONY: all

build-docker-airflow:
	docker build -t radiant-airflow:latest .

build-docker-k8s-operator:
	docker build -f Dockerfile-k8s-operator -t radiant-k8s-operator:latest .

build-docker: build-docker-airflow build-docker-k8s-operator

install:
	pip install -r requirements.txt

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
	ruff check radiant/ tests/

test: test-static test-unit test-integration

format:
	ruff format radiant/ tests/
	ruff check --fix radiant/ tests/
