# Financial Tracker – Local-First Data Pipeline

This project explores a fully local-first approach to extracting transaction histories using Plaid, storing the result in Delta format, and performing additional transformations for analytics.

The long-term goal is to automate the ingestion → enrichment → analytics workflow so the project behaves like a personal financial tracker app, but with richer analytical metrics and significantly lower cost (essentially only Plaid API usage).

## End-to-End Architecture

```
→ Raw JSON
→ Canonical Normalization
→ Delta Lake
→ DuckDB
→ Google Sheets (user enrichment)
→ Silver Layer (post-enrichment extraction)
→ Gold Layer (final transformations)
→ Metabase / BI Tool
→ Dashboards
→ Alerts / Weekly–Monthly Reports
```

---

## 1. Fetch Transactions from Plaid Sandbox

The pipeline begins by using the Plaid Quickstart to retrieve example transaction data.

The script `fetch_transactions.py` retrieves transactions and stores the full JSON response as:

```
data/raw/plaidsandbox/transactions_<timestamp>.json
```

### Key Properties

* Each run creates its own timestamped snapshot — no overwrites.
* Raw payloads remain immutable for perfect reproducibility.

---

## 2. Normalize and Validate Raw JSON

Raw API responses are transformed into a canonical structure using:

```
src/financial_tracker/normalization.py
```

Normalization produces consistent `CanonicalTransaction` objects.

Each canonical record is validated against:

```
config/schemas/transactions.schema.yaml
```

### Validation Checks

* Required fields
* Correct data types
* Consistent schema for downstream storage and analysis

This ensures all normalized data is clean, predictable, and safe for analytics.

---

## 3. Persist Canonical Data to Delta Lake

Validated canonical records are stored in a local Delta Lake table located at:

```
data/delta/transactions/
```

Implemented in:

```
src/storage/delta_writer.py
```

### Delta Lake Features

* Partitioned by `txn_year` and `txn_month`
* Idempotent upserts using `transaction_id`
* Local ACID storage suitable for incremental analytics

---

## 4. Query Data with DuckDB

Delta Lake data can be queried directly using DuckDB:

```sql
SELECT *
FROM delta_scan('data/delta/transactions');
```

DuckDB provides fast OLAP-style querying for local workloads and interactive analysis.

---

## 5. Sync Enriched Data to Google Sheets

A Google Sheets sync provides a human-enrichment layer where users can add manual labels, categories, and notes.

Only new canonical rows—based on a `pulled_at` watermark—are appended.

### Sync Behavior

* Append-only; never overwrites existing data
* Preserves user-entered columns (tags, categories, notes, etc.)
* Logs sync metadata to:

```
data/metadata/sheets_sync_log.jsonl
```

Implemented in:

```
src/financial_tracker/google_sync.py
```

---

If you’d like, I can extend this with:

* Setup instructions (`poetry`, kernels, pre-commit, environment vars)
* Diagrams (ASCII or mermaid)
* CLI usage examples
* Development workflow and file structure
* Future roadmap section

Just let me know and I’ll add it.
