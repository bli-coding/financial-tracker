from pathlib import Path
from typing import Literal

import pandas as pd
from deltalake import DeltaTable, write_deltalake

# Columns that must exist in the canonical frame
REQUIRED_COLUMNS = {"transaction_id", "date"}  # <-- matches canonical schema

PartitionMode = Literal["year_month"]


class DeltaWriter:
    """
    Simple local Delta Lake writer using delta-rs (deltalake).

    - Writes a canonical transactions DataFrame to a Delta table on disk
    - Partitions by year / month (derived from `date` column)
    - Idempotent upserts based on `transaction_id` (last-write-wins)

    Intended usage:

        writer = DeltaWriter("data/delta/transactions")
        writer.write(canonical_df)

    Where `canonical_df` is a pandas.DataFrame that already passed schema
    validation and includes at least:
        - transaction_id (string or int)
        - date (string or datetime-like; transaction date)
    """

    def __init__(
        self,
        table_path: str | Path,
        *,
        partition_mode: PartitionMode = "year_month",
        id_column: str = "transaction_id",
        date_column: str = "date",
    ) -> None:
        self.table_path = Path(table_path)
        self.partition_mode = partition_mode
        self.id_column = id_column
        self.date_column = date_column

    # --------------------
    # Public API
    # --------------------
    def write(self, df: pd.DataFrame) -> None:
        """
        Upsert `df` into the Delta table at `table_path`.

        - If the table does not exist yet -> create new, partitioned Delta table.
        - If the table exists -> load existing rows, upsert on `transaction_id`,
          then overwrite the table with the de-duplicated result.
        """
        if df.empty:
            print("DeltaWriter: nothing to write (empty DataFrame).")
            return

        self._ensure_required_columns(df)
        df = self._prepare_frame(df)

        if not self._table_exists():
            self._initial_write(df)
        else:
            self._merge_overwrite(df)

    # --------------------
    # Internal helpers
    # --------------------
    def _table_exists(self) -> bool:
        # A Delta table has a _delta_log directory
        return (self.table_path / "_delta_log").exists()

    def _ensure_required_columns(self, df: pd.DataFrame) -> None:
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(
                f"DeltaWriter: DataFrame is missing required column(s): {sorted(missing)}"
            )

    def _prepare_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize schema before writing:

        - Ensure `date` is datetime
        - Add partition columns (year, month) if partition_mode == "year_month"
        """
        df = df.copy()

        # Normalize date column to pandas datetime
        df[self.date_column] = pd.to_datetime(df[self.date_column])

        if self.partition_mode == "year_month":
            df["txn_year"] = df[self.date_column].dt.year.astype("int16")
            df["txn_month"] = df[self.date_column].dt.month.astype("int8")

        return df

    def _initial_write(self, df: pd.DataFrame) -> None:
        """First-time write: create a new Delta table."""
        self.table_path.mkdir(parents=True, exist_ok=True)

        partition_cols: list[str] | None = (
            ["txn_year", "txn_month"] if self.partition_mode == "year_month" else None
        )

        print(f"DeltaWriter: creating new Delta table at {self.table_path} …")
        write_deltalake(
            str(self.table_path),
            df,
            mode="overwrite",  # create table
            partition_by=partition_cols,
        )
        print("DeltaWriter: initial write complete.")

    def _merge_overwrite(self, df_new: pd.DataFrame) -> None:
        """
        Idempotent upsert based on transaction_id:

        1. Load existing rows from the Delta table
        2. Drop any existing rows whose transaction_id appears in df_new
        3. Append df_new rows
        4. Overwrite the table with the combined result
        """
        print(f"DeltaWriter: loading existing Delta table from {self.table_path} …")
        dt = DeltaTable(str(self.table_path))
        df_existing = dt.to_pandas()

        # Make sure id column exists in existing table as well
        if self.id_column not in df_existing.columns:
            raise ValueError(
                f"DeltaWriter: existing table is missing id column '{self.id_column}'. "
                "Did the schema change unexpectedly?"
            )

        # Filter out any rows that will be replaced by df_new
        ids_new = set(df_new[self.id_column].astype(str))
        mask_keep = ~df_existing[self.id_column].astype(str).isin(ids_new)
        df_kept = df_existing.loc[mask_keep]

        combined = pd.concat([df_kept, df_new], ignore_index=True)

        # Recompute partition columns in case schema changed
        combined = self._prepare_frame(combined)

        partition_cols: list[str] | None = (
            ["txn_year", "txn_month"] if self.partition_mode == "year_month" else None
        )

        print(
            f"DeltaWriter: upserting {len(df_new)} rows "
            f"(kept {len(df_kept)} existing, total {len(combined)}) …"
        )

        write_deltalake(
            str(self.table_path),
            combined,
            mode="overwrite",  # full-table overwrite with de-duplicated rows
            partition_by=partition_cols,
        )
        print("DeltaWriter: upsert overwrite complete.")
