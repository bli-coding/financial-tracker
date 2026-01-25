import os
from pathlib import Path

import duckdb


def get_plaid_env() -> str:
    return os.environ.get("PLAID_ENV", "sandbox").strip().lower()


def get_duckdb_path() -> Path:
    # matches your .env default: DUCKDB_PATH=./data/finance.db
    return Path(os.environ.get("DUCKDB_PATH", "./data/finance.db"))


def silver_delta_path(plaid_env: str) -> Path:
    return Path("data/delta") / plaid_env / "silver" / "transactions_scd2"


def connect_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    # Delta support
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")

    return con


def create_views(con: duckdb.DuckDBPyConnection, silver_path: Path) -> None:
    # Use absolute path to avoid “current working directory” surprises
    sp = str(silver_path.resolve())

    con.execute(
        f"""
        CREATE OR REPLACE VIEW silver_transactions_all AS
        SELECT *
        FROM delta_scan('{sp}');
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW silver_transactions_current AS
        SELECT *
        FROM delta_scan('{sp}')
        WHERE is_active = TRUE;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW silver_transactions_current_including_deleted AS
        SELECT *
        FROM delta_scan('{sp}')
        WHERE is_current = TRUE;
        """
    )


def fetch_one(con: duckdb.DuckDBPyConnection, sql: str) -> int | None:
    return con.execute(sql).fetchone()[0]


def assert_true(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        msg = f"❌ ASSERT FAILED: {name}"
        if detail:
            msg += f"\n   {detail}"
        raise AssertionError(msg)
    print(f"✅ {name}")


def run_assertions(con: duckdb.DuckDBPyConnection, silver_path: Path) -> None:
    sp = str(silver_path.resolve())

    # 0) table exists + readable
    total_rows = fetch_one(con, f"SELECT COUNT(*) FROM delta_scan('{sp}')")
    assert_true("Silver table is readable", total_rows is not None and total_rows >= 0, f"rows={total_rows}")

    if total_rows == 0:
        print("⚠️  Silver is empty. Assertions that need data will be skipped.")
        return

    # 1) For each (item_id, transaction_id), at most one row is_current=true
    max_curr = fetch_one(
        con,
        f"""
        SELECT COALESCE(MAX(curr_cnt), 0)
        FROM (
            SELECT item_id, transaction_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS curr_cnt
            FROM delta_scan('{sp}')
            GROUP BY 1, 2
        )
        """,
    )
    assert_true(
        "At most 1 current row per (item_id, transaction_id)",
        (max_curr or 0) <= 1,
        f"max current rows for a key = {max_curr}",
    )

    # 2) is_active implies (is_current=true AND is_deleted=false)
    bad_active = fetch_one(
        con,
        f"""
        SELECT COUNT(*)
        FROM delta_scan('{sp}')
        WHERE is_active = TRUE
          AND (is_current <> TRUE OR is_deleted = TRUE)
        """,
    )
    assert_true(
        "is_active implies current + not deleted",
        (bad_active or 0) == 0,
        f"violations={bad_active}",
    )

    # 3) Non-current rows must have valid_to_ts populated (closed interval)
    bad_noncurrent = fetch_one(
        con,
        f"""
        SELECT COUNT(*)
        FROM delta_scan('{sp}')
        WHERE is_current = FALSE
          AND (valid_to_ts IS NULL OR CAST(valid_to_ts AS VARCHAR) = '')
        """,
    )
    assert_true(
        "Non-current rows have valid_to_ts",
        (bad_noncurrent or 0) == 0,
        f"violations={bad_noncurrent}",
    )

    # 4) Current rows should have valid_to_ts null/empty (open interval)
    bad_current = fetch_one(
        con,
        f"""
        SELECT COUNT(*)
        FROM delta_scan('{sp}')
        WHERE is_current = TRUE
          AND valid_to_ts IS NOT NULL
          AND CAST(valid_to_ts AS VARCHAR) <> ''
        """,
    )
    assert_true(
        "Current rows have no valid_to_ts",
        (bad_current or 0) == 0,
        f"violations={bad_current}",
    )

    # 5) Basic sanity: version starts at 1 for each key (if you use versioning)
    bad_version_min = fetch_one(
        con,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT item_id, transaction_id, MIN(version) AS min_v
            FROM delta_scan('{sp}')
            GROUP BY 1,2
        )
        WHERE min_v IS NULL OR min_v <> 1
        """,
    )
    assert_true(
        "Version min is 1 per (item_id, transaction_id)",
        (bad_version_min or 0) == 0,
        f"violations={bad_version_min}",
    )

    print(f"🎉 All assertions passed. Silver rows={total_rows}")


def main() -> int:
    plaid_env = get_plaid_env()
    db_path = get_duckdb_path()
    silver_path = silver_delta_path(plaid_env)

    print(f"PLAID_ENV: {plaid_env}")
    print(f"DuckDB:    {db_path}")
    print(f"Silver:    {silver_path}")

    if not (silver_path / "_delta_log").exists():
        print(f"❌ Silver Delta table not found at: {silver_path}")
        return 1

    con = connect_duckdb(db_path)

    print("▶️  Creating DuckDB views...")
    create_views(con, silver_path)
    print("✅ Views created:")
    print("   - silver_transactions_all")
    print("   - silver_transactions_current")
    print("   - silver_transactions_current_including_deleted")

    print("▶️  Running assertions on Silver...")
    run_assertions(con, silver_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
