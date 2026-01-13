import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import plaid
from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest


def get_client() -> plaid_api.PlaidApi:
    plaid_env = os.environ.get("PLAID_ENV", "sandbox").strip().lower()
    if plaid_env == "sandbox":
        host = plaid.Environment.Sandbox
    elif plaid_env == "development":
        host = plaid.Environment.Development
    else:
        host = plaid.Environment.Production

    configuration = plaid.Configuration(
        host=host,
        api_key={
            "clientId": os.environ["PLAID_CLIENT_ID"],
            "secret": os.environ["PLAID_SECRET"],
        },
    )
    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    with path.open("a") as f:
        f.write(json.dumps(obj, default=str) + "\n")


def backfill_transactions_sync(
    access_token: str,
    institution: str,
    item_id: str,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Returns: final_cursor, all_added, all_modified, all_removed
    """
    client = get_client()

    inst_slug = institution.strip().lower().replace(" ", "_")
    base = Path("data/metadata/plaid/items") / inst_slug / item_id
    ensure_dir(base)
    cursor_log = base / "cursor_log.jsonl"
    cursor_latest = base / "cursor_latest.json"

    next_cursor: str | None = None
    all_added: list[dict[str, Any]] = []
    all_modified: list[dict[str, Any]] = []
    all_removed: list[dict[str, Any]] = []

    page = 0
    while True:
        page += 1
        if next_cursor is None:
            req = TransactionsSyncRequest(access_token=access_token)
        else:
            req = TransactionsSyncRequest(access_token=access_token, cursor=next_cursor)

        resp = client.transactions_sync(req).to_dict()

        added = resp.get("added", []) or []
        modified = resp.get("modified", []) or []
        removed = resp.get("removed", []) or []
        has_more = bool(resp.get("has_more"))
        new_cursor = resp.get("next_cursor")

        # Auditable log (no access_token stored)
        append_jsonl(
            cursor_log,
            {
                "ts": datetime.utcnow().isoformat(),
                "page": page,
                "institution": institution,
                "item_id": item_id,
                "cursor_in": next_cursor,
                "cursor_out": new_cursor,
                "counts": {
                    "added": len(added),
                    "modified": len(modified),
                    "removed": len(removed),
                },
                "has_more": has_more,
            },
        )

        all_added.extend(added)
        all_modified.extend(modified)
        all_removed.extend(removed)

        if not new_cursor:
            # Plaid notes this can happen if initialization isn't complete yet
            # (you may need to retry later)
            raise RuntimeError("No next_cursor returned. Transactions may not be ready yet.")

        next_cursor = new_cursor

        if not has_more:
            break

    # Save latest cursor pointer
    cursor_latest.write_text(
        json.dumps({"item_id": item_id, "institution": institution, "next_cursor": next_cursor})
    )

    return next_cursor, all_added, all_modified, all_removed


def min_max_dates(transactions: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    dates: list[str] = [d for t in transactions if isinstance((d := t.get("date")), str) and d]
    return (min(dates), max(dates)) if dates else (None, None)


if __name__ == "__main__":
    # You already have this JSON list pattern for prod items:
    # PLAID_ITEMS_PROD_JSON='[{"institution":"chase","item_id":"...","access_token":"..."}]'
    items = json.loads(os.environ["PLAID_ITEMS_PROD_JSON"])
    item = items[0]  # Chase-only for now (later you can loop)

    final_cursor, added, modified, removed = backfill_transactions_sync(
        access_token=item["access_token"],
        institution=item["institution"],
        item_id=item["item_id"],
    )

    min_d, max_d = min_max_dates(added + modified)
    print("✅ Backfill complete.")
    print("Final cursor saved.")
    print("Earliest transaction date seen:", min_d)
    print("Latest transaction date seen:", max_d)
    print("Counts:", {"added": len(added), "modified": len(modified), "removed": len(removed)})
