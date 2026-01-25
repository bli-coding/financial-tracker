# Financial Tracker – Personal Spending Analytics Pipeline

## Introduction

This project is an ongoing effort to build a comprehensive, automated financial tracking system that collects historical transaction data from all credit cards and bank accounts, transforms it through a medallion architecture, and provides persistent, trackable data for spending analysis and monitoring.

**Goal**: Collect as far back as possible all financial transactions from all cards I have, transform them using medallion architecture to produce a history-trackable, data-persistent, and automated pipeline that continuously tracks financial spending. This enables understanding and monitoring spending behavior for better financial management.

**Final Vision**: Create an interactive dashboard that visualizes and monitors spending patterns in an easy-to-digest and interpretable format, providing actionable insights for financial decision-making.

---

## Architecture Overview

The pipeline follows a **medallion architecture** (Bronze → Silver → Gold) with the following flow:

```
Plaid API (Historical + Incremental)
    ↓
Raw JSON Pages (Immutable)
    ↓
Bronze Layer (Delta Lake - Event Log)
    ↓
Silver Layer (Delta Lake - SCD2)
    ↓
DuckDB Views (Query Interface)
    ↓
Google Sheets (Manual Enrichment)
    ↓
[Future: Gold Layer + Dashboard]
```

---

## Tech Stack & Pipeline Steps

### Step 1: Raw Data Ingestion from Plaid API

**Script**: `scripts/transactions_sync_ingest_raw.py`

**Technology**: 
- **Plaid Python SDK** (`plaid-python`) - Connects to Plaid API
- **Cursor-based pagination** - Retrieves historical transactions (up to 2 years) and incremental updates

**Process**:
- Uses Plaid's `transactions_sync` endpoint with cursor-based pagination
- Retrieves historical transactions going back up to 2 years for each connected account
- Stores raw JSON responses as immutable page files: `data/raw/plaidprod/{institution}/{item_id}/sync_pages/page_*.json`
- Tracks cursor state per item for incremental syncs: `data/metadata/plaid/items/{institution}/{item_id}/cursor_latest.json`
- Supports multiple institutions (Amex, Chase, Citi, Discover, Marcus, etc.)

**Key Features**:
- Idempotent: tracks processed pages to avoid re-processing
- Handles `added`, `modified`, and `removed` transaction events
- Maintains audit logs of all sync runs

---

### Step 2: Bronze Layer - Event Log Ingestion

**Script**: `scripts/bronze_ingest_sync_events.py`

**Technology**:
- **Delta Lake** (`deltalake`) - ACID-compliant storage layer
- **Pandas** - Data transformation

**Process**:
- Reads raw JSON pages from Step 1
- Transforms each page into structured event rows (one row per transaction event)
- Appends to Bronze Delta table: `data/delta/{PLAID_ENV}/bronze/plaid_transactions_sync_events`
- Partitions by `event_year` for efficient querying
- Tracks processed pages to ensure idempotency: `data/metadata/plaid/bronze_state/{PLAID_ENV}/{institution}/{item_id}/processed_pages.jsonl`

**Schema**:
- Preserves all transaction fields (amount, date, merchant, category, location, etc.)
- Includes provenance metadata (run_id, run_ts, institution, item_id, cursor info)
- Stores raw transaction JSON for full traceability
- Event types: `added`, `modified`, `removed`

---

### Step 3: Silver Layer - SCD2 Transformation

**Script**: `scripts/build_silver_transactions_scd2.py`

**Technology**:
- **Delta Lake** - Silver table storage
- **Pandas** - SCD2 logic implementation

**Process**:
- Reads from Bronze event log
- Builds **Slowly Changing Dimension Type 2 (SCD2)** table
- Handles transaction lifecycle:
  - `added` → Creates new version with `is_current=true`, `is_active=true`
  - `modified` → Closes previous version, creates new current version
  - `removed` → Creates tombstone version with `is_deleted=true`, `is_active=false`
- Writes to Silver Delta table: `data/delta/{PLAID_ENV}/silver/transactions_scd2`
- Partitions by `txn_year` for efficient time-based queries

**SCD2 Features**:
- `valid_from_ts` / `valid_to_ts` - Temporal validity windows
- `is_current` - Marks the current version for each transaction
- `is_active` - Current AND not deleted (for "real spending" queries)
- `version` - Incremental version number per transaction
- Full lineage tracking (source_event_id, source_run_id, etc.)

---

### Step 4: DuckDB Views & Validation

**Script**: `scripts/build_duckdb_views_and_assert_silver.py`

**Technology**:
- **DuckDB** - Fast OLAP query engine
- **Delta Lake extension** - Direct Delta table scanning

**Process**:
- Creates DuckDB database: `data/finance.db`
- Installs Delta extension for direct Delta table access
- Creates views on Silver table:
  - `silver_transactions_all` - All SCD2 versions
  - `silver_transactions_current` - Only active current transactions (`is_active=true`)
  - `silver_transactions_current_including_deleted` - Current versions including deleted (`is_current=true`)
- Runs assertions to validate SCD2 integrity:
  - At most one current row per (item_id, transaction_id)
  - `is_active` implies `is_current=true AND is_deleted=false`
  - Non-current rows have `valid_to_ts` populated
  - Current rows have `valid_to_ts` null

**Benefits**:
- Fast SQL queries without loading full tables into memory
- Direct Delta table access without ETL overhead
- Foundation for analytics and dashboard queries

---

### Step 5: Google Sheets Sync for Manual Enrichment

**Script**: `src/financial_tracker/google_sync.py`

**Technology**:
- **gspread** - Google Sheets API client
- **Google Service Account** - Authentication
- **DuckDB** - Query Silver table for unsynced rows

**Process**:
- Reads from Silver Delta table via DuckDB
- Uses `valid_from_ts` as watermark to track last synced row
- Incrementally appends only new rows to Google Sheets
- Supports two export modes:
  - `current` (default): Only `is_current=true` rows (recommended for "real spending")
  - `all`: All SCD2 versions including deletions
- Preserves user-entered columns (tags, categories, notes, etc.) via column alignment
- Logs sync metadata: `data/metadata/sheets_sync_log.jsonl`

**Use Case**:
- Manual enrichment: Add custom categories, tags, notes, budget codes
- Human review and validation
- Future: Pull enriched data back for Gold layer transformations

---

## File Structure

```
financial-tracker/
├── config/                          # Configuration files
│   └── schemas/                     # JSON schemas for validation
│
├── data/
│   ├── delta/                       # Delta Lake tables
│   │   └── {PLAID_ENV}/             # production, sandbox, development
│   │       ├── bronze/
│   │       │   └── plaid_transactions_sync_events/  # Event log
│   │       └── silver/
│   │           └── transactions_scd2/              # SCD2 table
│   │
│   ├── finance.db                    # DuckDB database
│   │
│   ├── metadata/                     # Pipeline metadata
│   │   ├── plaid/
│   │   │   ├── bronze_state/        # Processed pages tracking
│   │   │   └── items/                # Per-item cursor state
│   │   │       └── {institution}/{item_id}/
│   │   │           ├── cursor_latest.json
│   │   │           ├── cursor_log.jsonl
│   │   │           └── runs.jsonl
│   │   └── sheets_sync_log.jsonl     # Google Sheets sync log
│   │
│   └── raw/                          # Immutable raw JSON
│       └── plaidprod/                # Production Plaid data
│           └── {institution}/{item_id}/
│               └── sync_pages/       # Raw API response pages
│                   └── page_*.json
│
├── docs/                             # Documentation
│   ├── experiments.md
│   └── stage2_next_steps.md
│
├── scripts/                           # Pipeline execution scripts
│   ├── transactions_sync_ingest_raw.py      # Step 1: Plaid API → Raw JSON
│   ├── bronze_ingest_sync_events.py         # Step 2: Raw → Bronze
│   ├── build_silver_transactions_scd2.py    # Step 3: Bronze → Silver SCD2
│   └── build_duckdb_views_and_assert_silver.py  # Step 4: DuckDB views
│
├── src/
│   └── financial_tracker/
│       ├── __init__.py
│       ├── google_sync.py           # Step 5: Silver → Google Sheets
│       └── io/
│           ├── __init__.py
│           └── sheets_sink.py       # Google Sheets writer utilities
│
├── tests/                            # Test suite
│   └── __init__.py
│
├── Jupyter_Notebooks/                # Analysis notebooks
│   └── test.ipynb
│
├── Makefile                          # Build automation
├── pyproject.toml                    # Poetry dependencies
├── poetry.lock                       # Locked dependencies
├── mypy.ini                          # Type checking config
├── pytest.ini                        # Test configuration
├── ruff.toml                         # Linting configuration
└── README.md                         # This file
```

---

## Setup: Credentials & Environment Variables

This project uses `direnv` to manage environment variables. Secrets are loaded from `~/.secrets/financial-tracker.env` (outside the repo).

### Required Environment Variables

#### Plaid Configuration
- `PLAID_CLIENT_ID`: Your Plaid client ID (same for all environments)
- `PLAID_SECRET`: Your Plaid secret (same for all environments)
- `PLAID_ENV`: Environment to use - `"sandbox"`, `"development"`, or `"production"`
  - Set to `"production"` to hit the real Plaid API
  - Set to `"development"` for development environment
  - Defaults to `"sandbox"` if not set
- `PLAID_ACCESS_TOKEN`: Access token for sandbox environment
- `PLAID_ITEMS_PROD_JSON`: (Production only) JSON array of items:
  ```json
  [
    {"institution": "amex", "item_id": "...", "access_token": "..."},
    {"institution": "chase", "item_id": "...", "access_token": "..."}
  ]
  ```

#### Google Sheets Configuration
- `GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON`: Path to Google service account JSON key file
- `GOOGLE_SHEETS_SPREADSHEET_ID`: Google Sheets spreadsheet ID (from the URL)
- `SILVER_SHEET_TAB_NAME`: (Optional) Worksheet/tab name, defaults to `"Silver_Transactions"`
- `SHEETS_EXPORT_MODE`: (Optional) `"current"` or `"all"`, defaults to `"current"`

#### DuckDB Configuration
- `DUCKDB_PATH`: (Optional) Path to DuckDB database, defaults to `./data/finance.db`

### Example `~/.secrets/financial-tracker.env` structure:

```bash
# Plaid
PLAID_CLIENT_ID=your_client_id
PLAID_SECRET=your_secret
PLAID_ENV=production
PLAID_ITEMS_PROD_JSON='[{"institution":"amex","item_id":"...","access_token":"..."},...]'

# Google Sheets
GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON=~/.secrets/google-service-account.json
GOOGLE_SHEETS_SPREADSHEET_ID=your_spreadsheet_id
SILVER_SHEET_TAB_NAME=Silver_Transactions
SHEETS_EXPORT_MODE=current

# DuckDB
DUCKDB_PATH=./data/finance.db
```

---

## Pipeline Execution

### Typical Workflow

1. **Initial Backfill** (first run):
   ```bash
   # Step 1: Fetch historical transactions from Plaid
   python scripts/transactions_sync_ingest_raw.py
   
   # Step 2: Ingest raw pages into Bronze
   python scripts/bronze_ingest_sync_events.py
   
   # Step 3: Build Silver SCD2 table
   python scripts/build_silver_transactions_scd2.py
   
   # Step 4: Create DuckDB views and validate
   python scripts/build_duckdb_views_and_assert_silver.py
   
   # Step 5: Sync to Google Sheets
   python -m financial_tracker.google_sync
   ```

2. **Incremental Updates** (daily/weekly):
   ```bash
   # Same sequence - scripts are idempotent and only process new data
   python scripts/transactions_sync_ingest_raw.py      # Fetches only new transactions
   python scripts/bronze_ingest_sync_events.py         # Processes only new pages
   python scripts/build_silver_transactions_scd2.py    # Rebuilds Silver (overwrites)
   python scripts/build_duckdb_views_and_assert_silver.py
   python -m financial_tracker.google_sync            # Appends only new rows
   ```


## Future Roadmap

- **Gold Layer**: Business logic transformations, aggregations, and derived metrics
- **Dashboard**: Interactive visualization using Streamlit, Plotly Dash, or similar
- **Automation**: Scheduled pipeline runs (Prefect, Airflow, or cron)
- **Enrichment Loop**: Pull user-enriched data from Google Sheets back into Gold layer
- **Alerts**: Spending threshold alerts and budget monitoring
- **Reports**: Automated weekly/monthly spending reports

---

## Dependencies

See `pyproject.toml` for full dependency list. Key technologies:
- `plaid-python` - Plaid API integration
- `deltalake` - Delta Lake storage
- `duckdb` - OLAP query engine
- `pandas` - Data manipulation
- `gspread` - Google Sheets API
- `pyspark` - Spark integration (for future scale)

---

## Development

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Type checking
poetry run mypy src/

# Linting
poetry run ruff check src/ scripts/
```
