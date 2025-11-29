# Makefile

# These targets are not real files
.PHONY: lint format type-check test check pre-commit-install pre-commit clean setup

# ---------------------------
# Linting with Ruff
# ---------------------------
lint:
	poetry run ruff check src tests

# ---------------------------
# Auto-format with Ruff
# ---------------------------
format:
	poetry run ruff format src tests

# ---------------------------
# Static type checking with MyPy
# ---------------------------
type-check:
	poetry run mypy --config-file=mypy.ini src

# ---------------------------
# Run the test suite with pytest
# ---------------------------
test:
	poetry run pytest

# ---------------------------
# Run all quality checks (no tests)
# Useful before committing
# ---------------------------
check: lint format type-check

# ---------------------------
# Install pre-commit hooks (one-time per clone)
# ---------------------------
pre-commit-install:
	poetry run pre-commit install

# ---------------------------
# Run all pre-commit hooks on the entire repo
# Good before big pushes
# ---------------------------
pre-commit:
	poetry run pre-commit run --all-files

# ---------------------------
# Setup convenience: install deps + pre-commit hooks
# ---------------------------
setup: 
	poetry install
	poetry run pre-commit install

# ---------------------------
# Clean caches and compiled artifacts
# ---------------------------
clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +


.PHONY: fetch-plaid

# --------------------------------
# Pull a new Plaid snapshot and archive it
# --------------------------------
fetch-plaid:
	poetry run python src/financial_tracker/fetch_transactions.py


.PHONY: normalize-plaid

normalize-plaid:
	poetry run python src/financial_tracker/normalization.py

.PHONY: jupyter-lab

jupyter-lab:
	poetry run jupyter lab