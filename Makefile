.PHONY: all

install:
	pip install -r requirements.txt

test:
	ruff check dags/ tests/
	pytest tests/

test-unit:
	ruff check dags/ tests/
	pytest tests/unit/

test-integration:
	ruff check dags/ tests/
	pytest tests/integration

test-static:
	ruff check dags/ tests/

format:
	ruff format dags/ tests/
	ruff check --fix dags/ tests/
