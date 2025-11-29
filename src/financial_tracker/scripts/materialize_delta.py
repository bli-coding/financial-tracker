import pandas as pd

from financial_tracker.logging_utils import log_ingestion_run
from financial_tracker.normalization import normalize_latest
from financial_tracker.storage.delta_writer import DeltaWriter


def main():
    try:
        records = normalize_latest()  # List[dict]
        df = pd.DataFrame(records)

        writer = DeltaWriter("data/delta/transactions")
        writer.write(df)

        log_ingestion_run(
            source="plaidsandbox",
            status="success",
            rows=len(df),
            raw_file=df["raw_file_path"].iloc[0],
            delta_table_path="data/delta/transactions",
        )

    except Exception as e:
        log_ingestion_run(
            source="plaidsandbox",
            status="failed",
            rows=0,
            notes=str(e),
        )
        raise


if __name__ == "__main__":
    main()
