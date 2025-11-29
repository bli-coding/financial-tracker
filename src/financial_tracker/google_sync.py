import json
import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from financial_tracker.io.sheets_sink import append_dataframe_aligned_to_header

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DELTA_TABLE_PATH = PROJECT_ROOT / "data" / "delta" / "transactions"
METADATA_DIR = PROJECT_ROOT / "data" / "metadata"
SHEETS_SYNC_LOG = METADATA_DIR / "sheets_sync_log.jsonl"


def _get_spreadsheet_config() -> tuple[str, str]:
    """
    Get Google Sheets config from env.

    Required:
      - GOOGLE_SHEETS_SPREADSHEET_ID

    Optional:
      - RAW_SHEET_TAB_NAME (default: "Raw_Transactions")
    """
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID is not set. Add it to your secrets/env.")

    worksheet_name = os.getenv("RAW_SHEET_TAB_NAME", "Raw_Transactions")
    return spreadsheet_id, worksheet_name


def _get_last_synced_pulled_at() -> str | None:
    """
    Look at sheets_sync_log.jsonl and return the last synced 'pulled_at' watermark.

    Returns an ISO 8601 timestamp string, or None if no previous sync exists.
    """
    if not SHEETS_SYNC_LOG.exists():
        return None

    last_ts: str | None = None
    with SHEETS_SYNC_LOG.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = rec.get("last_synced_pulled_at")
            if ts and (last_ts is None or ts > last_ts):
                last_ts = ts
    return last_ts


def _log_sheet_sync_run(rows_synced: int, last_pulled_at: str | None) -> None:
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "rows_synced": rows_synced,
        "last_synced_pulled_at": last_pulled_at,
    }

    with SHEETS_SYNC_LOG.open("a") as f:
        f.write(json.dumps(record) + "\n")

    print(f"[sheets_sync_log] rows_synced={rows_synced}, last_synced_pulled_at={last_pulled_at}")


def _get_unsynced_transactions() -> pd.DataFrame:
    """
    Return a DataFrame of canonical transactions from the Delta table
    that have not yet been synced to Google Sheets.

    Uses 'pulled_at' as the watermark.
    """
    last_synced = _get_last_synced_pulled_at()

    con = duckdb.connect()
    con.install_extension("delta")
    con.load_extension("delta")

    base_query = f"""
        SELECT *
        FROM delta_scan('{DELTA_TABLE_PATH.as_posix()}')
    """

    where_clause = f"WHERE pulled_at > '{last_synced}'" if last_synced is not None else ""

    order_clause = "ORDER BY pulled_at, transaction_id"

    query = f"{base_query} {where_clause} {order_clause}"
    df = con.sql(query).df()
    return df


def sync_new_raw_rows_to_sheet() -> None:
    """
    Main entry point:

    - Read unsynced canonical rows from Delta (based on pulled_at watermark)
    - Append them to a Google Sheet, preserving all canonical columns
    - Do NOT clear or overwrite existing rows
    - Log the sync watermark for next time
    """
    spreadsheet_id, worksheet_name = _get_spreadsheet_config()

    df = _get_unsynced_transactions()
    if df.empty:
        print("sync_new_raw_rows_to_sheet: no new rows to sync.")
        _log_sheet_sync_run(rows_synced=0, last_pulled_at=_get_last_synced_pulled_at())
        return

    # Append to Google Sheet
    append_dataframe_aligned_to_header(df, spreadsheet_id, worksheet_name)

    # Compute new watermark as max pulled_at from this batch
    last_pulled_at = df["pulled_at"].max()
    _log_sheet_sync_run(rows_synced=len(df), last_pulled_at=last_pulled_at)


if __name__ == "__main__":
    # Allow: poetry run python -m financial_tracker.google_sync
    sync_new_raw_rows_to_sheet()
