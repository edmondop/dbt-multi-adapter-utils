.PHONY: help install test lint lint-fix format typecheck clean all integration-tests

.DEFAULT_GOAL := help

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies with uv
	@echo "Installing dependencies with uv..."
	uv sync --extra dev
	@echo "Done! uv will automatically manage the environment"

test: ## Run pytest with coverage
	@echo "Running tests with coverage..."
	uv run pytest

lint: ## Run ruff check on source code
	@echo "Running ruff check..."
	uv run ruff check src/ tests/ integration_tests/

lint-fix: ## Fix linting problems automatically with ruff
	@echo "Fixing linting problems..."
	uv run ruff check --fix src/ tests/ integration_tests/

format: ## Format code with ruff
	@echo "Running ruff format..."
	uv run ruff format src/ tests/ integration_tests/

format-check: ## Check code formatting without modifying files
	@echo "Checking code formatting..."
	uv run ruff format --check src/ tests/ integration_tests/

typecheck: ## Run pyrefly type checking
	@echo "Running pyrefly type checking..."
	uv run pyrefly check src/

clean: ## Remove cache and build artifacts
	@echo "Cleaning cache and build artifacts..."
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Clean complete!"

integration-tests: ## Run dbt integration tests with Spark and DuckDB using testcontainers
	@echo "Running integration tests..."
	uv run pytest integration_tests/ -v --no-cov

integration-tests-fast: ## Run only fast dbt integration tests (DuckDB, no Spark)
	@echo "Running fast integration tests (DuckDB only)..."
	uv run pytest integration_tests/ -v -m "not slow" --no-cov

all: lint typecheck test ## Run all checks (lint, typecheck, test)
	@echo "All checks passed!"
