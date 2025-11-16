# Makefile

# Tell make these are not real files
.PHONY: lint format type-check test clean install run

# ---------------------------
# Linting with Ruff
# ---------------------------
lint:
	ruff check .

# ---------------------------
# Auto-format with Ruff
# ---------------------------
format:
	ruff format .

# ---------------------------
# Static Type Checking
# ---------------------------
type-check:
	mypy financial_tracker_demo

# ---------------------------
# PyTest
# ---------------------------
test:
	pytest -q --disable-warnings --maxfail=1

# ---------------------------
# Install project dependencies
# (useful when onboarding machines/CI)
# ---------------------------
install:
	poetry install

# ---------------------------
# Common cleanup
# ---------------------------
clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -r {} +

# ---------------------------
# Run your application
# (replace main.py with your entry point)
# ---------------------------
run:
	python -m financial_tracker_demo
