.PHONY: all

install:
	pip install -r requirements.txt

install-dev: install
	pip install -r requirements-dev.txt

test-unit:
	pytest tests/unit/

test-integration:
	pytest tests/integration

test-static:
	ruff check radiant/ tests/

test: test-static test-unit test-integration

format:
	ruff format radiant/ tests/
	ruff check --fix radiant/ tests/
