import json
from datetime import datetime
from pathlib import Path
from typing import Any

METADATA_DIR = Path("data/metadata")
LOG_PATH = METADATA_DIR / "ingestion_log.jsonl"


def log_ingestion_run(
    *,
    source: str,
    status: str,
    rows: int,
    raw_file: str | None = None,
    delta_table_path: str | None = None,
    notes: str | None = None,
) -> None:
    """
    Append a single-line ingestion metadata record.

    Produces newline-delimited JSON (JSONL) such as:

      {"timestamp": "...", "source": "plaidsandbox", "rows": 24,
       "status": "success", "raw_file": "...", "delta_table_path": "..."}

    Parameters:
        source: human-readable source name (“plaidsandbox”)
        status: “success”, “failed”, “partial”, etc.
        rows: number of canonical rows produced / written
        raw_file: optional path to raw Plaid file
        delta_table_path: optional path to delta table used
        notes: optional free-form string for debugging
    """

    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    record: dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(),
        "source": source,
        "status": status,
        "rows": rows,
        "raw_file": raw_file,
        "delta_table_path": delta_table_path,
        "notes": notes,
    }

    with LOG_PATH.open("a") as f:
        f.write(json.dumps(record) + "\n")

    print(
        f"[ingestion_log] wrote run: status={status}, rows={rows}, "
        f"raw_file={raw_file}, delta={delta_table_path}"
    )
