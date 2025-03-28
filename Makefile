.PHONY: all install test lint clean

# Default target
all: install test lint

# Install dependencies
install:
	pip install -r requirements.txt

# Run tests
test:
	black --check dags/ tests/
	pytest tests/

# Lint the code
format:
	black dags/ tests/
