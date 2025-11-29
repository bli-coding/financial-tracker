import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from jsonschema import ValidationError, validate

# Absolute path to THIS file
HERE = Path(__file__).resolve()

# Go up from src/financial_tracker/ to project root
PROJECT_ROOT = HERE.parents[2]

SCHEMA_PATH = PROJECT_ROOT / "config" / "schemas" / "transactions.schema.yaml"
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "plaidsandbox"


@dataclass
class CanonicalTransaction:
    # --- metadata ---
    source: str
    raw_file_path: str
    pulled_at: str

    # --- item / institution / account context ---
    item_id: str | None
    institution_id: str | None
    institution_name: str | None

    account_id: str
    account_name: str | None
    account_mask: str | None
    account_type: str | None
    account_subtype: str | None
    iso_currency_code: str | None

    # --- transaction core ---
    transaction_id: str
    date: str
    authorized_date: str | None
    amount: float
    name: str | None
    merchant_name: str | None
    counterparty_name: str | None

    # --- categories / labels ---
    category_primary: str | None
    category_detailed: str | None
    category_confidence_level: str | None

    # --- status / channel ---
    pending: bool
    payment_channel: str | None
    website: str | None
    logo_url: str | None


def _load_json(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _build_account_index(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    accounts = raw.get("accounts", [])
    return {a["account_id"]: a for a in accounts if "account_id" in a}


def _extract_item_info(raw: dict[str, Any]) -> dict[str, str | None]:
    item = raw.get("item") or {}
    return {
        "item_id": item.get("item_id"),
        "institution_id": item.get("institution_id"),
        "institution_name": item.get("institution_name"),
    }


def normalize_file(path: Path) -> list[dict[str, Any]]:
    """
    Normalize a single Plaid raw JSON file into a list of canonical transaction dicts.
    Schema is defined by CanonicalTransaction above.
    """
    raw = _load_json(path)
    acct_index = _build_account_index(raw)
    item_info = _extract_item_info(raw)

    pulled_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat()

    txs = raw.get("transactions", [])
    normalized: list[dict[str, Any]] = []

    for tx in txs:
        account_id = tx["account_id"]
        acct = acct_index.get(account_id, {})

        # pick currency from tx first, fall back to account
        iso_currency_code = tx.get("iso_currency_code") or acct.get("balances", {}).get(
            "iso_currency_code"
        )

        # first counterparty name (if present)
        counterparties = tx.get("counterparties") or []
        counterparty_name = counterparties[0]["name"] if counterparties else None

        pfc = tx.get("personal_finance_category") or {}
        category_primary = pfc.get("primary")
        category_detailed = pfc.get("detailed")
        category_confidence_level = pfc.get("confidence_level")

        canon = CanonicalTransaction(
            source="plaidsandbox",
            raw_file_path=str(path),
            pulled_at=pulled_at,
            item_id=item_info["item_id"],
            institution_id=item_info["institution_id"],
            institution_name=item_info["institution_name"],
            account_id=account_id,
            account_name=acct.get("name"),
            account_mask=acct.get("mask"),
            account_type=acct.get("type"),
            account_subtype=acct.get("subtype"),
            iso_currency_code=iso_currency_code,
            transaction_id=tx["transaction_id"],
            date=tx["date"],
            authorized_date=tx.get("authorized_date"),
            amount=float(tx["amount"]),
            name=tx.get("name"),
            merchant_name=tx.get("merchant_name"),
            counterparty_name=counterparty_name,
            category_primary=category_primary,
            category_detailed=category_detailed,
            category_confidence_level=category_confidence_level,
            pending=bool(tx.get("pending", False)),
            payment_channel=tx.get("payment_channel"),
            website=tx.get("website"),
            logo_url=tx.get("logo_url"),
        )

        record = asdict(canon)
        validate_record(record)
        normalized.append(record)

    return normalized


def load_schema():
    with SCHEMA_PATH.open() as f:
        return yaml.safe_load(f)


SCHEMA = load_schema()


def validate_record(record: dict):
    try:
        validate(instance=record, schema=SCHEMA)
    except ValidationError as e:
        raise ValueError(f"Record failed schema validation: {e.message}") from e


def normalize_latest() -> list[dict[str, Any]]:
    """
    Find the most recent raw Plaid file and normalize it.
    """
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"No raw directory found at {RAW_DIR}")

    candidates = list(RAW_DIR.glob("transactions_*.json"))
    if not candidates:
        raise FileNotFoundError(f"No raw Plaid JSON files found in {RAW_DIR}")

    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return normalize_file(latest)


def normalize_all() -> list[dict[str, Any]]:
    """
    Normalize all raw Plaid JSON files into a single list of canonical records.
    """
    if not RAW_DIR.exists():
        return []

    records: list[dict[str, Any]] = []
    for path in sorted(RAW_DIR.glob("transactions_*.json")):
        records.extend(normalize_file(path))
    return records


if __name__ == "__main__":
    records = normalize_latest()
    print(f"Normalized {len(records)} transactions from latest Plaid dump.")

    # optional: write small preview
    preview_path = Path("data/normalized_plaidsandbox_preview.json")
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text(json.dumps(records[:20], indent=2))
    print(f"Wrote preview of first 20 records to {preview_path}")
