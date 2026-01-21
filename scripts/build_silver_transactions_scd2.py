import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from deltalake import DeltaTable, write_deltalake

# ============================================================
# Silver SCD2 (overwrite) from Bronze (append-only event log)
# ============================================================
# - Reads Bronze Delta table: plaid_transactions_sync_events
# - Builds an SCD2 Silver table keyed by (item_id, transaction_id)
# - Handles added / modified / removed:
#     * added/modified => new version becomes current; previous version closes
#     * removed => new "tombstone" version becomes current with is_deleted=True
# - "Current spending" view: WHERE is_active = true
#     (is_active means current AND not deleted)
# - Overwrites Silver each run (simple + debuggable). Revisit later for incremental.


# ----------------------------
# Config / schema
# ----------------------------
SILVER_COLUMNS: list[str] = [
    # keys
    "plaid_env",
    "institution",
    "item_id",
    "transaction_id",
    # refined transaction fields
    "account_id",
    "amount",
    "iso_currency_code",
    "date",
    "authorized_date",
    "name",
    "merchant_name",
    "pending",
    "payment_channel",
    "pfc_primary",
    "pfc_detailed",
    "pfc_confidence",
    "city",
    "region",
    "country",
    # scd2
    "valid_from_ts",
    "valid_to_ts",
    "is_current",
    "is_active",
    "is_deleted",
    "change_type",
    "version",
    # lineage (audit)
    "source_event_id",
    "source_event_type",
    "source_run_id",
    "source_run_ts",
    "source_page_path",
    "silver_built_at",
    # optional convenience partitions
    "txn_year",
    "txn_month",
]

SILVER_DTYPES: dict[str, str] = {
    "plaid_env": "string",
    "institution": "string",
    "item_id": "string",
    "transaction_id": "string",
    "account_id": "string",
    "amount": "Float64",
    "iso_currency_code": "string",
    "date": "string",
    "authorized_date": "string",
    "name": "string",
    "merchant_name": "string",
    "pending": "boolean",
    "payment_channel": "string",
    "pfc_primary": "string",
    "pfc_detailed": "string",
    "pfc_confidence": "string",
    "city": "string",
    "region": "string",
    "country": "string",
    "valid_from_ts": "string",
    "valid_to_ts": "string",
    "is_current": "boolean",
    "is_active": "boolean",
    "is_deleted": "boolean",
    "change_type": "string",
    "version": "Int64",
    "source_event_id": "string",
    "source_event_type": "string",
    "source_run_id": "string",
    "source_run_ts": "string",
    "source_page_path": "string",
    "silver_built_at": "string",
    "txn_year": "Int64",
    "txn_month": "Int64",
}


# ----------------------------
# Utils
# ----------------------------
def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def get_plaid_env() -> str:
    return os.environ.get("PLAID_ENV", "sandbox").strip().lower()


def bronze_table_path(plaid_env: str) -> Path:
    return Path("data/delta") / plaid_env / "bronze" / "plaid_transactions_sync_events"


def silver_table_path(plaid_env: str) -> Path:
    return Path("data/delta") / plaid_env / "silver" / "transactions_scd2"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def parse_iso(ts: str) -> datetime:
    # accepts "2026-01-13T02:52:47.037978Z" or with +00:00
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def normalize_bool(v: Any) -> bool | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "t", "1", "yes", "y"):
            return True
        if s in ("false", "f", "0", "no", "n"):
            return False
    return None


def safe_str(v: Any) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v)
    return s if s != "" else None


def rows_to_stable_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)

    # Ensure stable columns
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Drop unexpected columns (keep table tidy)
    extra_cols = [c for c in df.columns if c not in SILVER_COLUMNS]
    if extra_cols:
        df = df.drop(columns=extra_cols)

    df = df[SILVER_COLUMNS]

    # Coerce dtypes
    for col, dtype in SILVER_DTYPES.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception:
            df[col] = df[col].astype("string")

    return df


# ----------------------------
# Read Bronze
# ----------------------------
def read_bronze_df(bronze_path: Path) -> pd.DataFrame:
    if not (bronze_path / "_delta_log").exists():
        raise FileNotFoundError(f"Bronze Delta table not found at: {bronze_path}")

    dt = DeltaTable(str(bronze_path))
    df = dt.to_pandas()  # fine for your scale

    # Minimal required columns
    required = {"item_id", "transaction_id", "event_type", "run_ts", "event_id"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Bronze table missing required columns: {sorted(missing)}")

    # Normalize key columns to string early
    df["item_id"] = df["item_id"].astype(str)
    df["transaction_id"] = df["transaction_id"].astype(str)
    df["event_type"] = df["event_type"].astype(str)
    df["event_id"] = df["event_id"].astype(str)

    # Ensure ordering columns exist
    if "ingested_at" not in df.columns:
        df["ingested_at"] = df["run_ts"]

    # Parse run_ts for ordering
    df["__run_ts_dt"] = df["run_ts"].apply(lambda x: parse_iso(str(x)) if pd.notna(x) else datetime.min.replace(tzinfo=UTC))
    df["__ingested_dt"] = df["ingested_at"].apply(lambda x: parse_iso(str(x)) if pd.notna(x) else datetime.min.replace(tzinfo=UTC))

    # Sort so SCD2 is deterministic: by time then stable tie-breaker
    df = df.sort_values(
        by=["item_id", "transaction_id", "__run_ts_dt", "__ingested_dt", "event_id"],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    return df


# ----------------------------
# SCD2 builder
# ----------------------------
def bronze_row_to_silver_payload(b: pd.Series) -> dict[str, Any]:
    """
    Build the refined "payload" for a transaction version.
    This intentionally excludes raw_transaction_json and cursor fields.
    """
    return {
        "plaid_env": safe_str(b.get("plaid_env")),
        "institution": safe_str(b.get("institution")),
        "item_id": safe_str(b.get("item_id")),
        "transaction_id": safe_str(b.get("transaction_id")),
        "account_id": safe_str(b.get("account_id")),
        "amount": b.get("amount"),
        "iso_currency_code": safe_str(b.get("iso_currency_code")),
        "date": safe_str(b.get("date")),
        "authorized_date": safe_str(b.get("authorized_date")),
        "name": safe_str(b.get("name")),
        "merchant_name": safe_str(b.get("merchant_name")),
        "pending": normalize_bool(b.get("pending")),
        "payment_channel": safe_str(b.get("payment_channel")),
        "pfc_primary": safe_str(b.get("pfc_primary")),
        "pfc_detailed": safe_str(b.get("pfc_detailed")),
        "pfc_confidence": safe_str(b.get("pfc_confidence")),
        "city": safe_str(b.get("city")),
        "region": safe_str(b.get("region")),
        "country": safe_str(b.get("country")),
    }


def build_silver_scd2_from_bronze(df_bronze: pd.DataFrame) -> list[dict[str, Any]]:
    """
    For each (item_id, transaction_id), walk events in time order and produce SCD2 versions.

    Semantics:
    - Each event produces a new version row (including 'removed' tombstone).
    - Previous current version (if any) gets closed: valid_to_ts = this_event_ts and is_current=false.
    - New version is_current=true.
    - is_active = is_current AND NOT is_deleted
    """
    built_at = utc_now_iso()
    out: list[dict[str, Any]] = []

    # group by natural key (include item_id so multiple items don't collide)
    key_cols = ["item_id", "transaction_id"]
    for (_item_id, _transaction_id), g in df_bronze.groupby(key_cols, sort=False):
        current_idx: int | None = None
        version = 0

        for _, b in g.iterrows():
            event_type = str(b["event_type"]).lower().strip()
            # Use run_ts as event time anchor (you can change later)
            event_ts = str(b.get("run_ts") or built_at)

            # Close prior current row if exists
            if current_idx is not None:
                out[current_idx]["valid_to_ts"] = event_ts
                out[current_idx]["is_current"] = False
                out[current_idx]["is_active"] = False  # not current anymore

            version += 1

            if event_type in ("added", "modified"):
                payload = bronze_row_to_silver_payload(b)
                is_deleted = False
            elif event_type == "removed":
                # Tombstone row: keep key + lineage; payload fields may be null
                payload = {
                    "plaid_env": safe_str(b.get("plaid_env")),
                    "institution": safe_str(b.get("institution")),
                    "item_id": safe_str(b.get("item_id")),
                    "transaction_id": safe_str(b.get("transaction_id")),
                    # all refined tx fields unknown -> null
                    "account_id": None,
                    "amount": None,
                    "iso_currency_code": None,
                    "date": None,
                    "authorized_date": None,
                    "name": None,
                    "merchant_name": None,
                    "pending": None,
                    "payment_channel": None,
                    "pfc_primary": None,
                    "pfc_detailed": None,
                    "pfc_confidence": None,
                    "city": None,
                    "region": None,
                    "country": None,
                }
                is_deleted = True
            else:
                # Unknown event types: skip (or raise if you prefer)
                continue

            row: dict[str, Any] = {
                **payload,
                "valid_from_ts": event_ts,
                "valid_to_ts": None,  # open-ended until closed
                "is_current": True,
                "is_deleted": is_deleted,
                "is_active": (not is_deleted),  # because it is_current=true for this new row
                "change_type": event_type,
                "version": version,
                "source_event_id": safe_str(b.get("event_id")),
                "source_event_type": safe_str(b.get("event_type")),
                "source_run_id": safe_str(b.get("run_id")),
                "source_run_ts": safe_str(b.get("run_ts")),
                "source_page_path": safe_str(b.get("page_path")),
                "silver_built_at": built_at,
                "txn_year": None,
                "txn_month": None,
            }

            # Add txn_year/month if date exists
            try:
                if row.get("date"):
                    d = datetime.fromisoformat(str(row["date"]))
                    row["txn_year"] = d.year
                    row["txn_month"] = d.month
            except Exception:
                # keep null; date may be missing or not ISO
                pass

            out.append(row)
            current_idx = len(out) - 1

        # After last event, current row remains open with valid_to_ts = null.

        # Ensure is_active means "current AND not deleted"
        if current_idx is not None:
            out[current_idx]["is_active"] = bool(out[current_idx]["is_current"]) and (not bool(out[current_idx]["is_deleted"]))

    return out


# ----------------------------
# Write Silver (overwrite)
# ----------------------------
def write_silver(delta_path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(delta_path)

    df = rows_to_stable_df(rows)

    # Overwrite the entire Silver table each run.
    # This is intentional for now: easy to debug & re-run.
    write_deltalake(
        str(delta_path),
        df,
        mode="overwrite",
        partition_by=["txn_year"],  # simple
        schema_mode="overwrite",  # keep schema stable across rebuilds
    )


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    plaid_env = get_plaid_env()

    bronze_path = bronze_table_path(plaid_env)
    silver_path = silver_table_path(plaid_env)

    print("▶️  Building Silver SCD2 from Bronze")
    print(f"   PLAID_ENV:   {plaid_env}")
    print(f"   Bronze:      {bronze_path}")
    print(f"   Silver(out): {silver_path}")

    df_bronze = read_bronze_df(bronze_path)
    print(f"   Bronze rows: {len(df_bronze)}")

    rows = build_silver_scd2_from_bronze(df_bronze)
    print(f"   Silver rows: {len(rows)}")

    write_silver(silver_path, rows)

    print("✅ Silver rebuild complete.")
