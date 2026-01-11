# Stage 2: Data Retrieval & Storage Implementation Steps

## Overview
Now that you've completed the Plaid quickstart POC and have your `access_token` and `item_id`, the next stage involves:
1. Building a Plaid client wrapper to retrieve data
2. Setting up local database (DuckDB) storage
3. Integrating Google Sheets API for data export
4. Creating an ingestion pipeline that orchestrates everything

---

## Step 1: Implement Plaid Client Wrapper

**Location**: `src/financial_tracker/plaid_client.py`

**Tasks**:
- Create a `PlaidClient` class that wraps the Plaid Python SDK
- Implement methods to:
  - Fetch accounts (`accounts_get`)
  - Fetch transactions using `transactions_sync` (recommended for incremental updates)
  - Fetch account balances (`accounts_balance_get`)
  - Handle errors and retries
- Use your existing `access_token` from the quickstart POC

**Key Implementation Notes**:
- Use `TransactionsSyncRequest` with cursor-based pagination (as shown in quickstart)
- Store the cursor to enable incremental syncs
- Handle rate limits and API errors gracefully

---

## Step 2: Set Up Local Database Schema

**Location**: `src/financial_tracker/database/`

**Tasks**:
- Create `schema.py` to define DuckDB table schemas:
  - `accounts` table (account_id, name, type, balance, etc.)
  - `transactions` table (transaction_id, account_id, amount, date, category, etc.)
  - `sync_state` table (item_id, cursor, last_sync_date) for tracking sync progress
- Create `db_init.py` to initialize DuckDB database and create tables
- Store database file in `data/warehouse/financial_tracker.duckdb`

**Schema Reference**: Use `config/schemas/transactions.schema.yaml` as a guide

---

## Step 3: Create Data Transformation Layer

**Location**: `src/financial_tracker/transformers/`

**Tasks**:
- Create `plaid_transformer.py` with functions to:
  - Normalize Plaid account responses → database schema
  - Normalize Plaid transaction responses → database schema
  - Handle nested fields (location, category arrays, etc.)
  - Convert date/timestamp formats
- Ensure data types match DuckDB schema

---

## Step 4: Implement Database Writer

**Location**: `src/financial_tracker/storage/db_writer.py`

**Tasks**:
- Create `DatabaseWriter` class that:
  - Connects to DuckDB
  - Writes accounts (upsert based on account_id)
  - Writes transactions (upsert based on transaction_id)
  - Updates sync_state table with cursor and last_sync_date
  - Handles duplicate key conflicts gracefully

**Dependencies**: `duckdb` package (already installed)

---

## Step 5: Set Up Google Sheets API Integration

**Location**: `src/financial_tracker/storage/sheets_writer.py`

**Tasks**:
- Install Google Sheets API dependency: `poetry add gspread google-auth`
- Set up Google Cloud credentials:
  - Create a service account in Google Cloud Console
  - Download JSON credentials file
  - Store credentials path in `.env` (e.g., `GOOGLE_SHEETS_CREDENTIALS_PATH`)
  - Share your Google Sheet with the service account email
- Create `SheetsWriter` class that:
  - Authenticates using service account credentials
  - Writes accounts to a "Accounts" sheet
  - Writes transactions to a "Transactions" sheet
  - Handles appending vs. updating existing rows
  - Formats data appropriately (dates, currencies)

**Configuration**:
- Add `GOOGLE_SHEET_ID` to your `.env` file
- Create a Google Sheet and note its ID from the URL

---

## Step 6: Create Ingestion Script

**Location**: `src/financial_tracker/ingestion/job.py`

**Tasks**:
- Create `IngestionJob` class that:
  - Initializes PlaidClient, DatabaseWriter, and SheetsWriter
  - Retrieves access_token from environment or config
  - Fetches accounts from Plaid → writes to DB → writes to Sheets
  - Fetches transactions from Plaid (using cursor from sync_state) → writes to DB → writes to Sheets
  - Updates sync_state after successful sync
  - Logs progress and errors
- Create a CLI entry point (`__main__.py` or `cli.py`) to run the job

**Error Handling**:
- Retry logic for API failures
- Partial failure handling (if DB write succeeds but Sheets fails, log but continue)
- Validation of data before writing

---

## Step 7: Create Configuration Management

**Location**: `src/financial_tracker/config.py`

**Tasks**:
- Create a configuration class that:
  - Loads environment variables (Plaid credentials, DB path, Sheets config)
  - Validates required settings
  - Provides defaults where appropriate
- Use `python-dotenv` to load from `.envrc` managed secrets

---

## Step 8: Test End-to-End Flow

**Tasks**:
- Create a test script or notebook to:
  1. Initialize database
  2. Run ingestion job with your existing `access_token`
  3. Verify data in DuckDB (query tables)
  4. Verify data in Google Sheets
  5. Run incremental sync (should only fetch new transactions)

**Testing Checklist**:
- [ ] Accounts are retrieved and stored correctly
- [ ] Transactions are retrieved and stored correctly
- [ ] Cursor is saved and used for next sync
- [ ] Data appears in Google Sheets with proper formatting
- [ ] Duplicate transactions are handled (upsert logic works)
- [ ] Error handling works (simulate API failure)

---

## Step 9: Add Logging and Monitoring

**Location**: `src/financial_tracker/utils/logging.py`

**Tasks**:
- Set up structured logging (use Python `logging` module)
- Log:
  - Number of accounts/transactions fetched
  - Write operations (success/failure)
  - API call durations
  - Errors with stack traces
- Optionally write logs to `logs/` directory

---

## Step 10: Create Makefile Commands (Optional)

**Location**: `Makefile`

**Tasks**:
- Add commands for:
  - `make init-db` - Initialize database schema
  - `make ingest` - Run ingestion job
  - `make test-sync` - Test sync with sample data
  - `make query-db` - Open DuckDB CLI for manual queries

---

## Dependencies to Add

Run these commands in your poetry environment:

```bash
# Google Sheets integration
poetry add gspread google-auth

# Optional: for better date handling
poetry add python-dateutil
```

---

## File Structure After Implementation

```
src/financial_tracker/
├── __init__.py
├── config.py                    # Configuration management
├── plaid_client.py              # Plaid API wrapper
├── database/
│   ├── __init__.py
│   ├── schema.py                # DuckDB table definitions
│   └── db_init.py               # Database initialization
├── transformers/
│   ├── __init__.py
│   └── plaid_transformer.py     # Data normalization
├── storage/
│   ├── __init__.py
│   ├── db_writer.py             # DuckDB writer
│   └── sheets_writer.py         # Google Sheets writer
├── ingestion/
│   ├── __init__.py
│   └── job.py                   # Main ingestion orchestration
└── utils/
    ├── __init__.py
    └── logging.py                # Logging setup

data/
└── warehouse/
    └── financial_tracker.duckdb  # DuckDB database file

logs/                              # Log files (gitignored)
```

---

## Next Steps After Stage 2

Once Stage 2 is complete, you'll be ready for:
- **Stage 3**: Automation (scheduling daily syncs with Prefect/cron)
- **Stage 4**: Analytics layer (creating views and aggregations)
- **Stage 5**: BI integration (connecting Metabase or similar)

---

## Quick Start Command Sequence

Once implemented, you should be able to run:

```bash
# 1. Initialize database
python -m financial_tracker.database.db_init

# 2. Run ingestion (uses access_token from .env)
python -m financial_tracker.ingestion.job

# 3. Query database
duckdb data/warehouse/financial_tracker.duckdb -c "SELECT * FROM transactions LIMIT 10;"
```



