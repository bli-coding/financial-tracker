import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import plaid
from plaid.api import plaid_api
from plaid.exceptions import ApiException
from plaid.model.transactions_sync_request import TransactionsSyncRequest


# ----------------------------
# Utils
# ----------------------------
def utc_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def iso_now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def json_default(o: Any) -> str:
    # Plaid responses sometimes include datetimes; stringify safely.
    if isinstance(o, (datetime,)):
        return o.isoformat()
    return str(o)


def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_\-]", "", s)
    return s or "unknown"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, default=json_default) + "\n")


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, default=json_default), encoding="utf-8")


# ----------------------------
# Plaid client
# ----------------------------
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
        api_key={"clientId": os.environ["PLAID_CLIENT_ID"], "secret": os.environ["PLAID_SECRET"]},
    )
    api_client = plaid.ApiClient(configuration)
    api_client.rest_client.pool_manager.connection_pool_kw["timeout"] = 10  # seconds
    return plaid_api.PlaidApi(api_client)


# ----------------------------
# Items resolution
# ----------------------------
def get_items() -> list[dict[str, str]]:
    """
    production: uses PLAID_ITEMS_PROD_JSON (list of {institution,item_id,access_token})
    non-production: uses PLAID_ACCESS_TOKEN (single)
    """
    plaid_env = os.environ.get("PLAID_ENV", "sandbox").strip().lower()
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


# ----------------------------
# Plaid error payload
# ----------------------------


def plaid_error_payload(e: Exception) -> dict[str, Any]:
    """
    Normalize Plaid errors into a JSON-serializable dict.
    Works for ApiException and other exceptions.
    """
    payload: dict[str, Any] = {
        "type": e.__class__.__name__,
        "message": str(e),
    }

    if isinstance(e, ApiException):
        payload["status"] = getattr(e, "status", None)
        payload["reason"] = getattr(e, "reason", None)

        body = getattr(e, "body", None)
        if body:
            try:
                body_json = json.loads(body) if isinstance(body, str) else body
                payload["plaid"] = {
                    "error_type": body_json.get("error_type"),
                    "error_code": body_json.get("error_code"),
                    "error_message": body_json.get("error_message"),
                    "display_message": body_json.get("display_message"),
                    "request_id": body_json.get("request_id"),
                    "documentation_url": body_json.get("documentation_url"),
                    "suggested_action": body_json.get("suggested_action"),
                }
                payload["plaid_raw"] = body_json
            except Exception:
                payload["body"] = body
        else:
            payload["body"] = None

    return payload


# ----------------------------
# Core ingestion per item
# ----------------------------
def item_paths(institution: str, item_id: str) -> dict[str, Path]:
    inst = slugify(institution)
    item = item_id  # keep original for folder; safe enough (Plaid item_id is URL-safe)
    base_raw = Path("data/raw/plaidprod") / inst / item
    pages_dir = base_raw / "sync_pages"

    base_meta = Path("data/metadata/plaid/items") / inst / item
    cursor_latest = base_meta / "cursor_latest.json"
    cursor_log = base_meta / "cursor_log.jsonl"
    runs_log = base_meta / "runs.jsonl"

    return {
        "pages_dir": pages_dir,
        "cursor_latest": cursor_latest,
        "cursor_log": cursor_log,
        "runs_log": runs_log,
        "base_raw": base_raw,
        "base_meta": base_meta,
    }


def load_cursor(cursor_latest_path: Path) -> str | None:
    data = read_json(cursor_latest_path)
    if not data:
        return None
    cur = data.get("next_cursor")
    if not cur:
        return None
    return str(cur)


def plaid_sync_call(
    client: plaid_api.PlaidApi,
    access_token: str,
    cursor: str | None,
) -> dict[str, Any]:
    try:
        if cursor is None:
            req = TransactionsSyncRequest(access_token=access_token)
        else:
            req = TransactionsSyncRequest(access_token=access_token, cursor=cursor)

        return client.transactions_sync(req).to_dict()

    except Exception as e:
        # Let caller decide what to do; but include structured error info.
        raise RuntimeError(f"Plaid sync error: {plaid_error_payload(e)}") from e


def ingest_item_raw_sync(
    client: plaid_api.PlaidApi,
    institution: str,
    item_id: str,
    access_token: str,
    sleep_if_empty_cursor_sec: float = 2.0,
) -> None:
    paths = item_paths(institution, item_id)
    ensure_dir(paths["pages_dir"])

    start_ts = utc_ts()
    start_iso = iso_now()

    cursor_in = load_cursor(paths["cursor_latest"])
    mode = "incremental" if cursor_in else "backfill"

    append_jsonl(
        paths["runs_log"],
        {
            "ts": start_iso,
            "run_id": start_ts,
            "institution": institution,
            "item_id": item_id,
            "mode": mode,
        },
    )

    cursor = cursor_in
    page = 0
    total_added = total_modified = total_removed = 0

    empty_cursor_polls = 0
    max_empty_cursor_polls = 3  # ~6 seconds with 2s sleep; tune as needed

    while True:
        page += 1

        try:
            resp = plaid_sync_call(client, access_token, cursor)
        except Exception as e:
            err = plaid_error_payload(e)

            # Log error with context
            append_jsonl(
                paths["runs_log"],
                {
                    "ts": iso_now(),
                    "run_id": start_ts,
                    "institution": institution,
                    "item_id": item_id,
                    "mode": mode,
                    "event": "error",
                    "cursor_in": cursor or "",
                    "page": page,
                    "error": err,
                },
            )
            append_jsonl(
                paths["cursor_log"],
                {
                    "ts": iso_now(),
                    "run_id": start_ts,
                    "institution": institution,
                    "item_id": item_id,
                    "page": page,
                    "cursor_in": cursor or "",
                    "cursor_out": "",
                    "counts": {"added": 0, "modified": 0, "removed": 0},
                    "has_more": False,
                    "note": "plaid_api_error",
                    "error": err,
                },
            )

            # Also print a readable summary to terminal
            plaid_info = err.get("plaid", {})
            print(
                f"❌ {institution} ({item_id}) failed on page={page} cursor='{cursor or ''}': "
                f"{plaid_info.get('error_code') or err['message']}"
            )
            if plaid_info.get("request_id"):
                print(f"   request_id: {plaid_info['request_id']}")

            # Controlled stop for this item
            break

        next_cursor = resp.get("next_cursor", "")

        if next_cursor == "":
            empty_cursor_polls += 1

            append_jsonl(
                paths["cursor_log"],
                {
                    "ts": iso_now(),
                    "run_id": start_ts,
                    "page": page,
                    "institution": institution,
                    "item_id": item_id,
                    "cursor_in": cursor or "",
                    "cursor_out": "",
                    "counts": {"added": 0, "modified": 0, "removed": 0},
                    "has_more": True,
                    "note": "next_cursor empty; polling",
                    "poll": empty_cursor_polls,
                    "max_polls": max_empty_cursor_polls,
                },
            )

            print(
                f"⏳ {institution} ({item_id}) next_cursor still empty "
                f"({empty_cursor_polls}/{max_empty_cursor_polls})..."
            )

            if empty_cursor_polls >= max_empty_cursor_polls:
                # Log and stop this item (no infinite loop)
                msg = (
                    f"next_cursor stayed empty after {max_empty_cursor_polls} polls "
                    f"({max_empty_cursor_polls * sleep_if_empty_cursor_sec:.0f}s)."
                )
                append_jsonl(
                    paths["runs_log"],
                    {
                        "ts": iso_now(),
                        "run_id": start_ts,
                        "institution": institution,
                        "item_id": item_id,
                        "mode": mode,
                        "event": "error",
                        "page": page,
                        "cursor_in": cursor or "",
                        "error": {"type": "EmptyCursorTimeout", "message": msg},
                    },
                )
                print(f"❌ {institution} ({item_id}) giving up: {msg}")
                break

            time.sleep(sleep_if_empty_cursor_sec)
            page -= 1
            continue

        # Once we get a real cursor, reset polling counter
        empty_cursor_polls = 0

        added = resp.get("added", []) or []
        modified = resp.get("modified", []) or []
        removed = resp.get("removed", []) or []
        has_more = bool(resp.get("has_more"))

        total_added += len(added)
        total_modified += len(modified)
        total_removed += len(removed)

        # Write one raw JSON per page (no consolidation)
        out_path = paths["pages_dir"] / f"page_{page:04d}_{start_ts}.json"
        write_json(
            out_path,
            {
                "run_id": start_ts,
                "run_ts": start_iso,
                "mode": mode,
                "institution": institution,
                "item_id": item_id,
                "plaid_env": os.environ.get("PLAID_ENV", "sandbox"),
                "cursor_in": cursor or "",
                "cursor_out": next_cursor,
                "has_more": has_more,
                # raw payload bits (no access_token)
                "added": added,
                "modified": modified,
                "removed": removed,
            },
        )

        # Audit cursor movement
        append_jsonl(
            paths["cursor_log"],
            {
                "ts": iso_now(),
                "run_id": start_ts,
                "page": page,
                "institution": institution,
                "item_id": item_id,
                "cursor_in": cursor or "",
                "cursor_out": next_cursor,
                "counts": {"added": len(added), "modified": len(modified), "removed": len(removed)},
                "has_more": has_more,
                "page_path": str(out_path),
            },
        )

        # Update cursor state after each successful page
        write_json(
            paths["cursor_latest"],
            {
                "updated_at": iso_now(),
                "run_id": start_ts,
                "institution": institution,
                "item_id": item_id,
                "next_cursor": next_cursor,
                "last_page_written": page,
                "mode": mode,
            },
        )

        cursor = next_cursor

        if not has_more:
            break

    # If we broke due to error before writing any final cursor, cursor_latest may still be absent.
    append_jsonl(
        paths["runs_log"],
        {
            "ts": iso_now(),
            "run_id": start_ts,
            "institution": institution,
            "item_id": item_id,
            "mode": mode,
            "event": "summary",
            "summary": {
                "pages": page,
                "added": total_added,
                "modified": total_modified,
                "removed": total_removed,
                "cursor_start": cursor_in or "",
                "cursor_end": cursor or "",
            },
        },
    )

    print(
        f"✅ {institution} ({item_id}) {mode} finished: pages={page}, "
        f"added={total_added}, modified={total_modified}, removed={total_removed}"
    )
    print(f"   Raw pages: {paths['pages_dir']}")
    print(f"   Cursor:    {paths['cursor_latest']}")


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    # This script is intended for production, but will work in other envs too.
    if "PLAID_CLIENT_ID" not in os.environ or "PLAID_SECRET" not in os.environ:
        raise RuntimeError("Missing PLAID_CLIENT_ID / PLAID_SECRET in environment.")

    client = get_client()
    items = get_items()

    for it in items:
        # be permissive about key casing

        institution = it.get("institution") or it.get("Institution") or "unknown"
        item_id = it.get("item_id") or it.get("itemId") or it.get("ITEM_ID")
        access_token = it.get("access_token") or it.get("accessToken") or it.get("ACCESS_TOKEN")

        if not item_id or not access_token:
            raise RuntimeError(f"Invalid item entry (missing item_id/access_token): {it}")

        print(f"▶️  Starting {institution} ({item_id}) ...")
        ingest_item_raw_sync(
            client=client,
            institution=str(institution),
            item_id=str(item_id),
            access_token=str(access_token),
        )
