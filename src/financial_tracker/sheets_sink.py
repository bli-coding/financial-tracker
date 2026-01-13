import os
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials


def get_gspread_client() -> gspread.Client:
    """
    Initialize a gspread client using a service account JSON file.

    Expects env var GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON to point
    to the JSON key file.
    """
    key_path = os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON")
    if not key_path:
        raise RuntimeError(
            "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON is not set. "
            "Point it to your service account JSON file."
        )

    key_file = Path(key_path).expanduser()
    if not key_file.exists():
        raise FileNotFoundError(f"Service account JSON file not found at {key_file}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds = Credentials.from_service_account_file(str(key_file), scopes=scopes)
    return gspread.authorize(creds)


def append_dataframe_aligned_to_header(
    df: pd.DataFrame,
    spreadsheet_id: str,
    worksheet_name: str,
) -> None:
    """
    Append rows from `df` to a worksheet without clearing existing data.

    Behavior:
      - If worksheet doesn't exist: create it, write header from df.columns, then append all rows.
      - If worksheet exists but header row is empty: set header from df.columns.
      - If worksheet exists with a header row:
          * Align df columns to that header (reorder + fill missing cols with "")
          * Append rows at the bottom, leaving any extra manual columns untouched.

    This lets you:
      - Store canonical columns in the sheet
      - Add extra columns manually (tags, notes, platform, etc.)
      - Keep re-running sync to only append *new* canonical rows
    """
    client = get_gspread_client()
    sh = client.open_by_key(spreadsheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        # Create worksheet and set header from df columns
        ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="50")
        header = list(df.columns)
        ws.update("A1", [header])
        print(f"Created worksheet '{worksheet_name}' with header from DataFrame.")
    else:
        # Worksheet exists; try to read header row
        header = ws.row_values(1)

        if not header:
            # Empty header row: initialize it from df.columns
            header = list(df.columns)
            ws.update("A1", [header])
            print(f"Initialized header in worksheet '{worksheet_name}' from DataFrame.")

    if df.empty:
        print("append_dataframe_aligned_to_header: DataFrame is empty, nothing to append.")
        return

    # Align df to header columns:
    #  - any missing header col in df => fill with ""
    #  - extra df columns ignored (not sent)
    aligned = df.copy()
    for col in header:
        if col not in aligned.columns:
            aligned[col] = ""

    aligned = aligned[header]  # same column order as the sheet header
    values: list[list[str]] = aligned.astype(str).values.tolist()

    ws.append_rows(values)
    print(
        f"append_dataframe_aligned_to_header: appended {len(values)} rows "
        f"to worksheet '{worksheet_name}'."
    )
