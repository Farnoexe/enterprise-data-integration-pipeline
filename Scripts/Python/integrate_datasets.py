import csv
import json
from datetime import date, datetime
from pathlib import Path


# -------------------------------------------------------
# Integration script
# Joins clean HR, IT, and Finance data into one
# analytics-ready output dataset.
#
# Join 1: IT -> HR on employee_id
#   - unmatched IT rows go to integration quarantine
#
# Join 2: matched HR+IT -> Finance on department
#   - unmatched rows are kept, Finance fields set to None
#
# Output:
#   Data/Integrated/integrated_department_assets_{timestamp}.csv
#   Data/Quarantine/integration/integration_quarantine_{timestamp}.csv
# -------------------------------------------------------


# Explicit field order for integrated output
# Defined here so CSV columns are stable and Athena table creation is straightforward
INTEGRATED_FIELDNAMES = [
    "employee_id",
    "employee_name",
    "department",
    "employee_location",
    "employee_status",
    "asset_id",
    "asset_type",
    "asset_status",
    "asset_location",
    "cost_center_id",
    "monthly_budget",
    "currency",
    "budget_month",
    "record_source_date",
]

# Explicit field order for integration quarantine
# Keeps original IT field names so rejected rows are easy to inspect
INTEGRATION_QUARANTINE_FIELDNAMES = [
    "asset_id",
    "employee_id",
    "asset_type",
    "assigned_date",
    "status",
    "location",
    "rejection_reason",
]


def get_project_root():
    # Script lives in Scripts/Python/ so we go up two levels
    return Path(__file__).resolve().parents[2]


def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_latest_file(folder, pattern):
    # Find the latest file based on timestamped filename
    files = sorted(folder.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {folder}")
    return files[-1]


def get_clean_dir(source_name):
    return get_project_root() / "Data" / "Clean" / source_name


def get_integrated_dir():
    integrated_dir = get_project_root() / "Data" / "Integrated"
    integrated_dir.mkdir(parents=True, exist_ok=True)
    return integrated_dir


def get_integration_quarantine_dir():
    # Kept separate from source-level quarantine files
    quarantine_dir = get_project_root() / "Data" / "Quarantine" / "integration"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    return quarantine_dir


def load_csv(file_path):
    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_json(file_path):
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_clean_hr():
    raw_file = get_latest_file(get_clean_dir("hr"), "hr_clean_*.csv")
    return load_csv(raw_file)


def load_clean_finance():
    raw_file = get_latest_file(get_clean_dir("finance"), "finance_clean_*.json")
    return load_json(raw_file)


def load_clean_it():
    raw_file = get_latest_file(get_clean_dir("it"), "it_clean_*.csv")
    return load_csv(raw_file)


def build_hr_lookup(hr_rows):
    # Build a dictionary keyed on employee_id for fast lookup
    lookup = {}
    for row in hr_rows:
        emp_id = row["employee_id"].strip()
        lookup[emp_id] = row
    return lookup


def build_finance_lookup(finance_records):
    # Build a dictionary keyed on department for fast lookup
    # One Finance record per department after validation
    lookup = {}
    for record in finance_records:
        dept = record["department"].strip()
        lookup[dept] = record
    return lookup


def integrate(it_rows, hr_lookup, finance_lookup):
    integrated_rows = []
    quarantine_rows = []

    # Today's date added to every integrated record for lineage
    record_source_date = date.today().isoformat()

    for it_row in it_rows:
        emp_id = it_row.get("employee_id", "").strip()

        # Join 1: match IT row to HR on employee_id
        hr_row = hr_lookup.get(emp_id)

        if hr_row is None:
            # No matching HR employee — quarantine this IT row
            quarantine_row = {
                "asset_id": it_row.get("asset_id", "").strip(),
                "employee_id": emp_id,
                "asset_type": it_row.get("asset_type", "").strip(),
                "assigned_date": it_row.get("assigned_date", "").strip(),
                "status": it_row.get("status", "").strip(),
                "location": it_row.get("location", "").strip(),
                "rejection_reason": "unmatched_employee_id",
            }
            quarantine_rows.append(quarantine_row)
            continue

        # Join 2: match HR department to Finance
        department = hr_row.get("department", "").strip()
        finance_row = finance_lookup.get(department)

        # If no Finance match, keep the row but leave Finance fields blank
        # This is acceptable — not every department may have a budget record
        integrated_row = {
            "employee_id": emp_id,
            "employee_name": hr_row.get("employee_name", "").strip(),
            "department": department,
            "employee_location": hr_row.get("location", "").strip(),
            "employee_status": hr_row.get("status", "").strip(),
            "asset_id": it_row.get("asset_id", "").strip(),
            "asset_type": it_row.get("asset_type", "").strip(),
            "asset_status": it_row.get("status", "").strip(),
            "asset_location": it_row.get("location", "").strip(),
            "cost_center_id": finance_row.get("cost_center_id") if finance_row else None,
            "monthly_budget": finance_row.get("monthly_budget") if finance_row else None,
            "currency": finance_row.get("currency") if finance_row else None,
            "budget_month": finance_row.get("budget_month") if finance_row else None,
            "record_source_date": record_source_date,
        }

        integrated_rows.append(integrated_row)

    return integrated_rows, quarantine_rows


def save_integrated_output(rows, output_path):
    if not rows:
        print("  No integrated rows to save.")
        return

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INTEGRATED_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def save_integration_quarantine(rows, output_path):
    if not rows:
        print("  No integration quarantine rows to save.")
        return

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INTEGRATION_QUARANTINE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def main():
    print("Starting integration...")
    print(f"Project root: {get_project_root()}")

    timestamp = get_timestamp()

    try:
        print("Loading clean source files...")
        hr_rows = load_clean_hr()
        finance_records = load_clean_finance()
        it_rows = load_clean_it()

        print(f"  HR clean records loaded: {len(hr_rows)}")
        print(f"  Finance clean records loaded: {len(finance_records)}")
        print(f"  IT clean records loaded: {len(it_rows)}")

    except FileNotFoundError as e:
        print(f"  Failed to load clean source files: {e}")
        return
    except (OSError, json.JSONDecodeError) as e:
        print(f"  Error reading clean source files: {e}")
        return

    # HR and IT are required — integration cannot proceed without them
    if not hr_rows or not it_rows:
        print("  Required clean inputs are empty. Integration cannot continue.")
        return

    # Build lookup dictionaries for efficient joining
    hr_lookup = build_hr_lookup(hr_rows)
    finance_lookup = build_finance_lookup(finance_records)

    # Run integration joins
    integrated_rows, quarantine_rows = integrate(it_rows, hr_lookup, finance_lookup)

    # Save integrated output
    integrated_path = get_integrated_dir() / f"integrated_department_assets_{timestamp}.csv"
    save_integrated_output(integrated_rows, integrated_path)

    # Save integration quarantine
    quarantine_path = get_integration_quarantine_dir() / f"integration_quarantine_{timestamp}.csv"
    save_integration_quarantine(quarantine_rows, quarantine_path)

    print("\nIntegration complete.")
    print("Summary:")
    print(f"  Clean IT records processed: {len(it_rows)}")
    print(f"  Integrated records produced: {len(integrated_rows)}")
    print(f"  Integration quarantine records: {len(quarantine_rows)}")
    print(f"  Integrated output: {integrated_path}")
    print(f"  Integration quarantine: {quarantine_path}")


if __name__ == "__main__":
    main()
