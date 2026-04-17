import csv
import json
from datetime import datetime
from pathlib import Path


# -------------------------------------------------------
# Source file locations (relative to project root)
# -------------------------------------------------------
HR_SOURCE_FILE = "Sources/hr_source.csv"
FINANCE_SOURCE_FILE = "Sources/finance_source.json"
IT_SOURCE_FILE = "Sources/it_source.csv"


def get_project_root():
    # Script lives in Scripts/Python/ so we go up two levels
    return Path(__file__).resolve().parents[2]


def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_raw_dir(source_name):
    raw_dir = get_project_root() / "Data" / "Raw" / source_name
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


def load_csv(file_path):
    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_json(file_path):
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_raw_csv(rows, output_path):
    if not rows:
        raise ValueError("No rows to save.")

    fieldnames = list(rows[0].keys())

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_raw_json(data, output_path):
    if not data:
        raise ValueError("No data to save.")

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def ingest_hr(timestamp):
    print("Ingesting HR source...")

    source_path = get_project_root() / HR_SOURCE_FILE
    rows = load_csv(source_path)

    output_path = get_raw_dir("hr") / f"hr_raw_{timestamp}.csv"
    save_raw_csv(rows, output_path)

    print(f"  HR records loaded: {len(rows)}")
    print(f"  Raw file saved to: {output_path}")

    return len(rows)


def ingest_finance(timestamp):
    print("Ingesting Finance source...")

    source_path = get_project_root() / FINANCE_SOURCE_FILE
    data = load_json(source_path)

    output_path = get_raw_dir("finance") / f"finance_raw_{timestamp}.json"
    save_raw_json(data, output_path)

    print(f"  Finance records loaded: {len(data)}")
    print(f"  Raw file saved to: {output_path}")

    return len(data)


def ingest_it(timestamp):
    print("Ingesting IT source...")

    source_path = get_project_root() / IT_SOURCE_FILE
    rows = load_csv(source_path)

    output_path = get_raw_dir("it") / f"it_raw_{timestamp}.csv"
    save_raw_csv(rows, output_path)

    print(f"  IT records loaded: {len(rows)}")
    print(f"  Raw file saved to: {output_path}")

    return len(rows)


def main():
    print("Starting source ingestion...")
    print(f"Project root: {get_project_root()}")

    timestamp = get_timestamp()

    hr_count = 0
    finance_count = 0
    it_count = 0

    try:
        hr_count = ingest_hr(timestamp)
    except FileNotFoundError as e:
        print(f"  HR ingestion failed - file not found: {e}")
    except (OSError, ValueError, json.JSONDecodeError) as e:
        print(f"  HR ingestion failed: {e}")

    try:
        finance_count = ingest_finance(timestamp)
    except FileNotFoundError as e:
        print(f"  Finance ingestion failed - file not found: {e}")
    except (OSError, ValueError, json.JSONDecodeError) as e:
        print(f"  Finance ingestion failed: {e}")

    try:
        it_count = ingest_it(timestamp)
    except FileNotFoundError as e:
        print(f"  IT ingestion failed - file not found: {e}")
    except (OSError, ValueError, json.JSONDecodeError) as e:
        print(f"  IT ingestion failed: {e}")

    print("Ingestion complete.")
    print("Summary:")
    print(f"  HR records ingested: {hr_count}")
    print(f"  Finance records ingested: {finance_count}")
    print(f"  IT records ingested: {it_count}")


if __name__ == "__main__":
    main()
