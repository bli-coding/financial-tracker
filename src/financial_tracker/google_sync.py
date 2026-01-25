"""
google_sync.py

Push Silver (SCD2) Plaid transactions into Google Sheets incrementally.

- Reads from Delta table:
    data/delta/{PLAID_ENV}/silver/transactions_scd2

- Appends ONLY new rows since last sync, using `valid_from_ts` as watermark.
- Optionally exports only the "current" view (recommended): is_current = TRUE
  so Sheets reflects your real spending at the moment.

Env vars required:
  GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON=~/.secrets/financial-tracker-sheets-key.json
  GOOGLE_SHEETS_SPREADSHEET_ID='...'

Optional:
  PLAID_ENV=production|sandbox|development  (defaults to sandbox if not set)
  SILVER_SHEET_TAB_NAME="Silver_Transactions" (default)
  SHEETS_EXPORT_MODE="current" | "all"
    - current: only is_current = TRUE rows (recommended)
    - all: export all SCD2 versions and deletions too
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from financial_tracker.io.sheets_sink import append_dataframe_aligned_to_header

# ----------------------------
# Paths & constants
# ----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _get_plaid_env() -> str:
    return os.getenv("PLAID_ENV", "sandbox").strip().lower()


def _silver_delta_path() -> Path:
    """
    Matches your repo structure in the screenshot:
      data/delta/production/silver/transactions_scd2
      data/delta/sandbox/silver/transactions_scd2
    """
    plaid_env = _get_plaid_env()
    return PROJECT_ROOT / "data" / "delta" / plaid_env / "silver" / "transactions_scd2"


METADATA_DIR = PROJECT_ROOT / "data" / "metadata"
SHEETS_SYNC_LOG = METADATA_DIR / "sheets_sync_log.jsonl"

# watermark column for silver SCD2
WATERMARK_COL = "valid_from_ts"


# ----------------------------
# Google Sheets config
# ----------------------------
def _get_spreadsheet_config() -> tuple[str, str]:
    """
    Required:
      - GOOGLE_SHEETS_SPREADSHEET_ID

    Optional:
      - SILVER_SHEET_TAB_NAME (default: "Silver_Transactions")
    """
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID is not set.")

    worksheet_name = os.getenv("SILVER_SHEET_TAB_NAME", "Silver_Transactions")
    return spreadsheet_id, worksheet_name


def _export_mode() -> str:
    """
    current: only is_current = TRUE rows (best for "my true spending right now")
    all: export every SCD2 version row (incl deletions)
    """
    mode = os.getenv("SHEETS_EXPORT_MODE", "current").strip().lower()
    if mode not in {"current", "all"}:
        raise RuntimeError("SHEETS_EXPORT_MODE must be 'current' or 'all'.")
    return mode


# ----------------------------
# Sync log (watermark)
# ----------------------------
def _get_last_synced_watermark() -> str | None:
    """
    Read sheets_sync_log.jsonl and return the last synced watermark for this env + tab + table.
    """
    if not SHEETS_SYNC_LOG.exists():
        return None

    plaid_env = _get_plaid_env()
    silver_path = _silver_delta_path().as_posix()
    _, worksheet_name = _get_spreadsheet_config()
    export_mode = _export_mode()

    last_ts: str | None = None
    with SHEETS_SYNC_LOG.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            # only consider log entries matching current env/sheet/table/mode
            if rec.get("plaid_env") != plaid_env:
                continue
            if rec.get("worksheet_name") != worksheet_name:
                continue
            if rec.get("silver_delta_path") != silver_path:
                continue
            if rec.get("export_mode") != export_mode:
                continue

            ts = rec.get("last_synced_valid_from_ts")
            if ts and (last_ts is None or ts > last_ts):
                last_ts = ts

    return last_ts


def _log_sheet_sync_run(rows_synced: int, last_watermark: str | None) -> None:
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    plaid_env = _get_plaid_env()
    silver_path = _silver_delta_path().as_posix()
    spreadsheet_id, worksheet_name = _get_spreadsheet_config()
    export_mode = _export_mode()

    record = {
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "plaid_env": plaid_env,
        "silver_delta_path": silver_path,
        "spreadsheet_id": spreadsheet_id,
        "worksheet_name": worksheet_name,
        "export_mode": export_mode,
        "rows_synced": rows_synced,
        "last_synced_valid_from_ts": last_watermark,
    }

    with SHEETS_SYNC_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    print(f"[sheets_sync_log] env={plaid_env} mode={export_mode} rows_synced={rows_synced}, last_synced_valid_from_ts={last_watermark}")


# ----------------------------
# Read from Delta via DuckDB
# ----------------------------
def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Extensions live inside DuckDB; this does NOT require a duckdb CLI binary
    con.install_extension("delta")
    con.load_extension("delta")
    return con


def _get_unsynced_silver_rows() -> pd.DataFrame:
    """
    Returns a DataFrame of rows from Silver Delta that have not been synced yet,
    using valid_from_ts as the watermark.

    Export behavior:
      - current mode: only is_current = TRUE
      - all mode: no filtering on is_current (you'll get versions + deletions)
    """
    delta_path = _silver_delta_path()
    if not (delta_path / "_delta_log").exists():
        raise RuntimeError(f"Silver Delta table not found at: {delta_path}")

    last_synced = _get_last_synced_watermark()
    export_mode = _export_mode()

    con = _connect_duckdb()

    # Important: your table stores timestamps as VARCHAR, so we keep string comparisons.
    # ISO 8601 strings compare correctly lexicographically, which is what we rely on.
    where_parts = []
    if export_mode == "current":
        where_parts.append("is_current = TRUE")

    if last_synced is not None:
        where_parts.append(f"{WATERMARK_COL} > '{last_synced}'")

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    # Make output stable and useful in Sheets
    order_clause = f"ORDER BY {WATERMARK_COL}, institution, item_id, transaction_id, version"

    query = f"""
        SELECT
          plaid_env,
          institution,
          item_id,
          account_id,
          transaction_id,
          date,
          authorized_date,
          amount,
          iso_currency_code,
          name,
          merchant_name,
          pending,
          payment_channel,
          pfc_primary,
          pfc_detailed,
          pfc_confidence,
          city,
          region,
          country,
          is_current,
          is_active,
          is_deleted,
          change_type,
          version,
          valid_from_ts,
          valid_to_ts,
          source_event_type,
          source_run_ts,
          silver_built_at
        FROM delta_scan('{delta_path.as_posix()}')
        {where_clause}
        {order_clause}
    """

    df = con.sql(query).df()
    return df


# ----------------------------
# Main sync
# ----------------------------
def sync_silver_rows_to_sheet() -> None:
    """
    - Read unsynced rows from Silver Delta (watermark: valid_from_ts)
    - Append to Google Sheet, aligning to header
    - Log watermark for next run
    """
    spreadsheet_id, worksheet_name = _get_spreadsheet_config()

    df = _get_unsynced_silver_rows()
    if df.empty:
        print("sync_silver_rows_to_sheet: no new rows to sync.")
        _log_sheet_sync_run(rows_synced=0, last_watermark=_get_last_synced_watermark())
        return

    append_dataframe_aligned_to_header(df, spreadsheet_id, worksheet_name)

    # New watermark is max(valid_from_ts) from this batch
    last_watermark = str(df[WATERMARK_COL].max())
    _log_sheet_sync_run(rows_synced=len(df), last_watermark=last_watermark)


if __name__ == "__main__":
    # Run:
    #   poetry run python -m financial_tracker.google_sync
    sync_silver_rows_to_sheet()
