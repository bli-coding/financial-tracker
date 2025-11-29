from jsonschema import ValidationError

from financial_tracker.normalization import normalize_latest


def main():
    try:
        records = normalize_latest()
        print(f"✅ {len(records)} records validated successfully.")
    except ValidationError as e:
        print(f"❌ Schema validation failed: {e.message}")


if __name__ == "__main__":
    main()
