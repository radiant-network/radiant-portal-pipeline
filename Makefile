.PHONY: all

install:
	pip install -r requirements.txt

test:
	ruff check radiant/ tests/
	pytest tests/

test-unit:
	ruff check radiant/ tests/
	pytest tests/unit/

test-integration:
	ruff check radiant/ tests/
	pytest tests/integration

test-static:
	ruff check radiant/ tests/

format:
	ruff format radiant/ tests/
	ruff check --fix radiant/ tests/
