.PHONY: all install test lint clean

# Default target
all: install test lint

# Install dependencies
install:
	pip install -r requirements.txt

# Run tests
test:
	pytest tests/

# Lint the code
lint:
	black --check dags/ tests/

# Clean up
clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -r {} +
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf htmlcov