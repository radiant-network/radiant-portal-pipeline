.PHONY: all

install:
	pip install -r requirements.txt

test:
	black --check dags/ tests/
	pytest tests/

format:
	black dags/ tests/
