import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions

PLAID_CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PLAID_SECRET = os.environ["PLAID_SECRET"]
PLAID_ENV = os.environ.get("PLAID_ENV", "sandbox").strip().lower()


def json_default(o: Any) -> str:
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_\-]", "", s)
    return s or "unknown"


def get_items() -> list[dict[str, str]]:
    """
    - production: PLAID_ITEMS_PROD_JSON = JSON list of {institution, item_id, access_token}
    - sandbox/development: PLAID_ACCESS_TOKEN (single)
    """
    if PLAID_ENV == "production":
        raw = os.environ.get("PLAID_ITEMS_PROD_JSON", "").strip()
        if not raw:
            raise RuntimeError("PLAID_ENV=production but PLAID_ITEMS_PROD_JSON is missing/empty.")

        items = json.loads(raw)
        if not isinstance(items, list) or not items:
            raise RuntimeError("PLAID_ITEMS_PROD_JSON must be a non-empty JSON list.")

        required = {"institution", "item_id", "access_token"}
        out: list[dict[str, str]] = []
        for i, it in enumerate(items):
            if not isinstance(it, dict) or not required.issubset(it.keys()):
                raise RuntimeError(f"PLAID_ITEMS_PROD_JSON[{i}] must have keys {sorted(required)}")
            out.append(
                {
                    "institution": str(it["institution"]),
                    "item_id": str(it["item_id"]),
                    "access_token": str(it["access_token"]),
                }
            )
        return out

    token = os.environ.get("PLAID_ACCESS_TOKEN", "").strip()
    if not token:
        raise RuntimeError("PLAID_ENV is not production, but PLAID_ACCESS_TOKEN is missing/empty.")
    return [{"institution": "sandbox", "item_id": "sandbox", "access_token": token}]


def get_client() -> plaid_api.PlaidApi:
    if PLAID_ENV == "sandbox":
        host = plaid.Environment.Sandbox
    elif PLAID_ENV == "development":
        host = plaid.Environment.Development
    else:
        host = plaid.Environment.Production

    configuration = plaid.Configuration(
        host=host, api_key={"clientId": PLAID_CLIENT_ID, "secret": PLAID_SECRET}
    )
    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


def fetch_tx_for_token(
    client: plaid_api.PlaidApi,
    access_token: str,
    start_date: date,
    end_date: date,
    count: int = 500,
) -> dict[str, Any]:
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        options=TransactionsGetRequestOptions(count=count, offset=0),
    )
    return client.transactions_get(request).to_dict()


def write_raw_dump(institution: str, item_id: str, data: dict[str, Any], timestamp: str) -> Path:
    if PLAID_ENV == "production":
        out_dir = Path("data/raw/plaidprod") / slugify(institution)
    else:
        out_dir = Path("data/raw/plaidsandbox")

    out_dir.mkdir(parents=True, exist_ok=True)

    safe_item = slugify(item_id)[:48]
    out_path = out_dir / f"transactions_{safe_item}_{timestamp}.json"

    with out_path.open("w") as f:
        json.dump(data, f, indent=2, default=json_default)

    return out_path


def append_experiments_log(
    institution: str, item_id: str, out_path: Path, timestamp: str, data: dict[str, Any]
) -> None:
    total_tx = data.get("total_transactions")
    accounts = data.get("accounts", [])
    account_names = [acct.get("name") or acct.get("official_name") for acct in accounts]

    log_path = Path("docs/experiments.md")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    label = "Plaid prod run" if PLAID_ENV == "production" else f"Plaid {PLAID_ENV} run"
    with log_path.open("a") as log:
        log.write(f"## {label} – {timestamp}\n")
        log.write(f"- Institution: {institution}\n")
        log.write(f"- Item ID: `{item_id}`\n")
        log.write(f"- JSON path: `{out_path}`\n")
        log.write(f"- Total transactions: {total_tx}\n")
        log.write(f"- Accounts: {', '.join(filter(None, account_names)) or 'N/A'}\n\n")


if __name__ == "__main__":
    items = get_items()

    # Start conservative (7 days); bump to 90 after validation
    end_date = date.today()
    start_date = end_date - timedelta(days=7)

    client = get_client()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for item in items:
        data = fetch_tx_for_token(
            client=client,
            access_token=item["access_token"],
            start_date=start_date,
            end_date=end_date,
        )

        out_path = write_raw_dump(
            institution=item["institution"],
            item_id=item["item_id"],
            data=data,
            timestamp=timestamp,
        )

        print(f"✅ Saved Plaid {PLAID_ENV} dump to {out_path}")

        append_experiments_log(
            institution=item["institution"],
            item_id=item["item_id"],
            out_path=out_path,
            timestamp=timestamp,
            data=data,
        )
