import json
import os
import re
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
from deltalake import write_deltalake

# ============================
# Bronze schema (stable)
# ============================
# Keep this list stable across runs to avoid schema drift.
BRONZE_COLUMNS: list[str] = [
    # run / provenance
    "plaid_env",
    "run_id",
    "run_ts",
    "event_date",
    "event_year",
    "event_month",
    "mode",
    "institution",
    "item_id",
    "cursor_in",
    "cursor_out",
    "has_more",
    "page_path",
    "event_type",
    # identifiers
    "transaction_id",
    "event_id",
    # readable tx fields
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
    # raw payload
    "raw_transaction_json",
    "ingested_at",
]

# Pandas dtypes that avoid NullType and keep columns stable even if all-null.
# Use pandas nullable dtypes for robustness.
BRONZE_DTYPES: dict[str, str] = {
    "plaid_env": "string",
    "run_id": "string",
    "run_ts": "string",
    "event_date": "string",
    "event_year": "Int64",
    "event_month": "Int64",
    "mode": "string",
    "institution": "string",
    "item_id": "string",
    "cursor_in": "string",
    "cursor_out": "string",
    "has_more": "boolean",
    "page_path": "string",
    "event_type": "string",
    "transaction_id": "string",
    "event_id": "string",
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
    "raw_transaction_json": "string",
    "ingested_at": "string",
}


# ----------------------------
# Utils
# ----------------------------
def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_\-]", "", s)
    return s or "unknown"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def parse_iso(ts: str) -> datetime:
    # expects "2026-01-13T02:52:47.037978Z" or similar
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def safe_get_transaction_id(tx: Any) -> str | None:
    # added/modified are dicts; removed can be dicts or strings depending on sdk version
    if isinstance(tx, str):
        return tx
    if isinstance(tx, dict):
        v = tx.get("transaction_id")
        return str(v) if v is not None else None
    return None


def stable_event_id(
    *,
    item_id: str,
    run_id: str,
    page_path: str,
    event_type: str,
    transaction_id: str,
) -> str:
    raw = f"{item_id}|{run_id}|{page_path}|{event_type}|{transaction_id}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _coalesce(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


# ----------------------------
# Path resolution
# ----------------------------
def get_plaid_env() -> str:
    return os.environ.get("PLAID_ENV", "sandbox").strip().lower()


def get_items() -> list[dict[str, str]]:
    """
    production: PLAID_ITEMS_PROD_JSON -> list of {institution,item_id,access_token}
    non-production: PLAID_ACCESS_TOKEN -> single token
    """
    plaid_env = get_plaid_env()
    if plaid_env == "production":
        raw = os.environ.get("PLAID_ITEMS_PROD_JSON", "").strip()
        if not raw:
            raise RuntimeError("PLAID_ENV=production but PLAID_ITEMS_PROD_JSON is missing/empty.")
        items = json.loads(raw)
        if not isinstance(items, list) or not items:
            raise RuntimeError("PLAID_ITEMS_PROD_JSON must be a non-empty JSON list.")
        return items

    token = os.environ.get("PLAID_ACCESS_TOKEN", "").strip()
    if not token:
        raise RuntimeError("PLAID_ACCESS_TOKEN missing for non-production env.")
    return [{"institution": "sandbox", "item_id": "sandbox", "access_token": token}]


def item_raw_pages_dir(plaid_env: str, institution: str, item_id: str) -> Path:
    inst = slugify(institution)
    if plaid_env == "production":
        return Path("data/raw/plaidprod") / inst / item_id / "sync_pages"
    return Path("data/raw/plaidsandbox") / inst / item_id / "sync_pages"


def item_bronze_state_dir(plaid_env: str, institution: str, item_id: str) -> Path:
    inst = slugify(institution)
    return Path("data/metadata/plaid/bronze_state") / plaid_env / inst / item_id


def processed_pages_path(plaid_env: str, institution: str, item_id: str) -> Path:
    return item_bronze_state_dir(plaid_env, institution, item_id) / "processed_pages.jsonl"


def bronze_table_path(plaid_env: str) -> Path:
    return Path("data/delta") / plaid_env / "bronze" / "plaid_transactions_sync_events"


# ----------------------------
# Idempotency: page-level processed set
# ----------------------------
def load_processed_pages(ppath: Path) -> set[str]:
    if not ppath.exists():
        return set()
    processed: set[str] = set()
    with ppath.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                page = obj.get("page_path")
                if page:
                    processed.add(str(page))
            except json.JSONDecodeError:
                continue
    return processed


def mark_page_processed(ppath: Path, page_path: Path, run_id: str) -> None:
    append_jsonl(
        ppath,
        {"ts": utc_now_iso(), "run_id": run_id, "page_path": str(page_path)},
    )


# ----------------------------
# Transform: raw sync page -> bronze rows
# ----------------------------
def extract_common_fields(page: dict[str, Any]) -> dict[str, Any]:
    run_ts = page.get("run_ts") or utc_now_iso()
    dt = parse_iso(run_ts)

    # Prefer page payload; fall back to env
    plaid_env = _coalesce(page.get("plaid_env"), get_plaid_env())

    return {
        "plaid_env": plaid_env,
        "run_id": page.get("run_id"),
        "run_ts": run_ts,
        "event_date": dt.date().isoformat(),
        "event_year": dt.year,
        "event_month": dt.month,
        "mode": page.get("mode"),
        "institution": page.get("institution"),
        "item_id": page.get("item_id"),
        "cursor_in": page.get("cursor_in"),
        "cursor_out": page.get("cursor_out"),
        "has_more": bool(page.get("has_more")),
    }


def tx_readable_columns(tx: dict[str, Any]) -> dict[str, Any]:
    # keep “readable”, minimal opinions
    pfc = tx.get("personal_finance_category") or {}
    loc = tx.get("location") or {}
    return {
        "account_id": tx.get("account_id"),
        "amount": tx.get("amount"),
        "iso_currency_code": tx.get("iso_currency_code"),
        "date": tx.get("date"),
        "authorized_date": tx.get("authorized_date"),
        "name": tx.get("name"),
        "merchant_name": tx.get("merchant_name"),
        "pending": tx.get("pending"),
        "payment_channel": tx.get("payment_channel"),
        "pfc_primary": pfc.get("primary"),
        "pfc_detailed": pfc.get("detailed"),
        "pfc_confidence": pfc.get("confidence_level"),
        "city": loc.get("city"),
        "region": loc.get("region"),
        "country": loc.get("country"),
    }


def build_bronze_rows_from_page(page_path: Path) -> tuple[str, list[dict[str, Any]]]:
    page = read_json(page_path)
    common = extract_common_fields(page)

    run_id = str(common.get("run_id") or "unknown")
    item_id = str(common.get("item_id") or "")
    rows: list[dict[str, Any]] = []

    # added / modified
    for event_type in ("added", "modified"):
        txs = page.get(event_type) or []
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            tx_id = safe_get_transaction_id(tx)
            if not tx_id:
                continue

            row = {
                **common,
                "page_path": str(page_path),
                "event_type": event_type,
                "transaction_id": str(tx_id),
                "event_id": stable_event_id(
                    item_id=item_id,
                    run_id=run_id,
                    page_path=str(page_path),
                    event_type=event_type,
                    transaction_id=str(tx_id),
                ),
                **tx_readable_columns(tx),
                "raw_transaction_json": json.dumps(tx, ensure_ascii=False),
                "ingested_at": utc_now_iso(),
            }
            rows.append(row)

    # removed
    removed = page.get("removed") or []
    for tx in removed:
        tx_id = safe_get_transaction_id(tx)
        if not tx_id:
            continue
        row = {
            **common,
            "page_path": str(page_path),
            "event_type": "removed",
            "transaction_id": str(tx_id),
            "event_id": stable_event_id(
                item_id=item_id,
                run_id=run_id,
                page_path=str(page_path),
                event_type="removed",
                transaction_id=str(tx_id),
            ),
            # readable columns unknown for removals (kept as nulls)
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
            "raw_transaction_json": None,
            "ingested_at": utc_now_iso(),
        }
        rows.append(row)

    return run_id, rows


# ----------------------------
# Delta write (append) with stable schema
# ----------------------------
def rows_to_stable_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Build a DataFrame with a stable column set and stable pandas dtypes
    to prevent delta-rs NullType issues and future schema uncertainty.
    """
    # Create df from rows first
    df = pd.DataFrame(rows)

    # Ensure all expected columns exist
    for col in BRONZE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Drop any unexpected columns unless you explicitly want to keep them.
    # (You can remove this if you'd like to allow arbitrary new cols.)
    extra_cols = [c for c in df.columns if c not in BRONZE_COLUMNS]
    if extra_cols:
        df = df.drop(columns=extra_cols)

    # Order columns deterministically
    df = df[BRONZE_COLUMNS]

    # Coerce to stable dtypes
    for col, dtype in BRONZE_DTYPES.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception:
            # Be conservative: fallback to string if coercion fails
            df[col] = df[col].astype("string")

    return df


def append_to_bronze_delta(delta_path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    df = rows_to_stable_df(rows)

    ensure_dir(delta_path)

    write_deltalake(
        str(delta_path),
        df,
        mode="append",
        partition_by=["event_year"],
        schema_mode="merge",  # allow adding new columns in future versions
    )


# ----------------------------
# Main per-item processing
# ----------------------------
def process_item(plaid_env: str, institution: str, item_id: str) -> None:
    pages_dir = item_raw_pages_dir(plaid_env, institution, item_id)
    if not pages_dir.exists():
        print(f"⚠️  No raw pages dir for {institution} ({item_id}): {pages_dir}")
        return

    pproc = processed_pages_path(plaid_env, institution, item_id)
    processed = load_processed_pages(pproc)

    all_pages = sorted(pages_dir.glob("page_*.json"))
    new_pages = [p for p in all_pages if str(p) not in processed]

    if not new_pages:
        print(f"✅ {institution} ({item_id}) bronze: no new raw pages.")
        return

    delta_path = bronze_table_path(plaid_env)

    total_rows = 0
    for page_path in new_pages:
        run_id, rows = build_bronze_rows_from_page(page_path)
        append_to_bronze_delta(delta_path, rows)
        mark_page_processed(pproc, page_path, run_id)
        total_rows += len(rows)

    print(f"✅ {institution} ({item_id}) bronze: processed {len(new_pages)} page(s), wrote {total_rows} row(s) -> {delta_path}")


if __name__ == "__main__":
    plaid_env = get_plaid_env()
    items = get_items()

    for it in items:
        institution = str(it.get("institution"))
        item_id = str(it.get("item_id"))
        if not item_id:
            print(f"⚠️  Skipping item missing item_id: {it}")
            continue

        process_item(plaid_env=plaid_env, institution=institution, item_id=item_id)
