import csv
import json
from datetime import datetime
from pathlib import Path


# -------------------------------------------------------
# Validation script for all three source systems
# Reads latest raw files, applies validation rules,
# splits records into clean and quarantine outputs
# -------------------------------------------------------


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


def get_raw_dir(source_name):
    return get_project_root() / "Data" / "Raw" / source_name


def get_clean_dir(source_name):
    clean_dir = get_project_root() / "Data" / "Clean" / source_name
    clean_dir.mkdir(parents=True, exist_ok=True)
    return clean_dir


def get_quarantine_dir(source_name):
    quarantine_dir = get_project_root() / "Data" / "Quarantine" / source_name
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    return quarantine_dir


def is_blank(value):
    return value is None or str(value).strip() == ""


def is_valid_date(value, fmt="%Y-%m-%d"):
    try:
        datetime.strptime(str(value).strip(), fmt)
        return True
    except ValueError:
        return False


def load_csv(file_path):
    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_json(file_path):
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_clean_csv(rows, output_path, fieldnames):
    if not rows:
        print(f"  No valid rows to save for {output_path.name}")
        return

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_quarantine_csv(rows, output_path, fieldnames):
    if not rows:
        print(f"  No quarantined rows to save for {output_path.name}")
        return

    quarantine_fieldnames = fieldnames + ["rejection_reason"]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=quarantine_fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_clean_json(records, output_path):
    if not records:
        print(f"  No valid records to save for {output_path.name}")
        return

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


def save_quarantine_json(records, output_path):
    # Records already include rejection_reason before being saved
    if not records:
        print(f"  No quarantined records to save for {output_path.name}")
        return

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


# -------------------------------------------------------
# HR Validation
# -------------------------------------------------------
def validate_hr(rows):
    valid_rows = []
    quarantine_rows = []
    seen_employee_ids = set()

    for row in rows:
        reasons = []

        if is_blank(row.get("employee_id")):
            reasons.append("missing_employee_id")

        if is_blank(row.get("department")):
            reasons.append("missing_department")

        if is_blank(row.get("hire_date")) or not is_valid_date(row.get("hire_date")):
            reasons.append("invalid_hire_date")

        if not is_blank(row.get("employee_id")):
            emp_id = row["employee_id"].strip()
            if emp_id in seen_employee_ids:
                reasons.append("duplicate_employee_id")
            else:
                seen_employee_ids.add(emp_id)

        if reasons:
            row_copy = row.copy()
            row_copy["rejection_reason"] = "; ".join(reasons)
            quarantine_rows.append(row_copy)
        else:
            valid_rows.append(row)

    return valid_rows, quarantine_rows


# -------------------------------------------------------
# Finance Validation
# -------------------------------------------------------
def validate_finance(records):
    valid_records = []
    quarantine_records = []
    seen_keys = set()

    for record in records:
        reasons = []

        if is_blank(record.get("department")):
            reasons.append("missing_department")

        if is_blank(record.get("budget_month")):
            reasons.append("missing_budget_month")

        try:
            budget = float(record.get("monthly_budget", ""))
            if budget <= 0:
                reasons.append("invalid_monthly_budget")
        except (ValueError, TypeError):
            reasons.append("invalid_monthly_budget")

        dept = str(record.get("department", "")).strip()
        month = str(record.get("budget_month", "")).strip()
        composite_key = f"{dept}|{month}"

        if dept and month:
            if composite_key in seen_keys:
                reasons.append("duplicate_department_budget_month")
            else:
                seen_keys.add(composite_key)

        if reasons:
            rec_copy = record.copy()
            rec_copy["rejection_reason"] = "; ".join(reasons)
            quarantine_records.append(rec_copy)
        else:
            valid_records.append(record)

    return valid_records, quarantine_records


# -------------------------------------------------------
# IT Validation
# -------------------------------------------------------
def validate_it(rows):
    valid_rows = []
    quarantine_rows = []
    seen_asset_ids = set()

    for row in rows:
        reasons = []

        if is_blank(row.get("asset_id")):
            reasons.append("missing_asset_id")

        if is_blank(row.get("employee_id")):
            reasons.append("missing_employee_id")

        if is_blank(row.get("assigned_date")) or not is_valid_date(row.get("assigned_date")):
            reasons.append("invalid_assigned_date")

        if not is_blank(row.get("asset_id")):
            asset_id = row["asset_id"].strip()
            if asset_id in seen_asset_ids:
                reasons.append("duplicate_asset_id")
            else:
                seen_asset_ids.add(asset_id)

        if reasons:
            row_copy = row.copy()
            row_copy["rejection_reason"] = "; ".join(reasons)
            quarantine_rows.append(row_copy)
        else:
            valid_rows.append(row)

    return valid_rows, quarantine_rows


# -------------------------------------------------------
# Source Wrappers
# -------------------------------------------------------
def validate_hr_source(timestamp):
    print("Validating HR source...")

    raw_file = get_latest_file(get_raw_dir("hr"), "hr_raw_*.csv")
    rows = load_csv(raw_file)

    if not rows:
        print("  No HR records found. Skipping.")
        return 0, 0, 0

    fieldnames = list(rows[0].keys())
    valid_rows, quarantine_rows = validate_hr(rows)

    clean_path = get_clean_dir("hr") / f"hr_clean_{timestamp}.csv"
    quarantine_path = get_quarantine_dir("hr") / f"hr_quarantine_{timestamp}.csv"

    save_clean_csv(valid_rows, clean_path, fieldnames)
    save_quarantine_csv(quarantine_rows, quarantine_path, fieldnames)

    print(f"  Total records: {len(rows)}")
    print(f"  Valid records: {len(valid_rows)}")
    print(f"  Quarantined records: {len(quarantine_rows)}")

    return len(rows), len(valid_rows), len(quarantine_rows)


def validate_finance_source(timestamp):
    print("Validating Finance source...")

    raw_file = get_latest_file(get_raw_dir("finance"), "finance_raw_*.json")
    records = load_json(raw_file)

    if not records:
        print("  No Finance records found. Skipping.")
        return 0, 0, 0

    valid_records, quarantine_records = validate_finance(records)

    clean_path = get_clean_dir("finance") / f"finance_clean_{timestamp}.json"
    quarantine_path = get_quarantine_dir("finance") / f"finance_quarantine_{timestamp}.json"

    save_clean_json(valid_records, clean_path)
    save_quarantine_json(quarantine_records, quarantine_path)

    print(f"  Total records: {len(records)}")
    print(f"  Valid records: {len(valid_records)}")
    print(f"  Quarantined records: {len(quarantine_records)}")

    return len(records), len(valid_records), len(quarantine_records)


def validate_it_source(timestamp):
    print("Validating IT source...")

    raw_file = get_latest_file(get_raw_dir("it"), "it_raw_*.csv")
    rows = load_csv(raw_file)

    if not rows:
        print("  No IT records found. Skipping.")
        return 0, 0, 0

    fieldnames = list(rows[0].keys())
    valid_rows, quarantine_rows = validate_it(rows)

    clean_path = get_clean_dir("it") / f"it_clean_{timestamp}.csv"
    quarantine_path = get_quarantine_dir("it") / f"it_quarantine_{timestamp}.csv"

    save_clean_csv(valid_rows, clean_path, fieldnames)
    save_quarantine_csv(quarantine_rows, quarantine_path, fieldnames)

    print(f"  Total records: {len(rows)}")
    print(f"  Valid records: {len(valid_rows)}")
    print(f"  Quarantined records: {len(quarantine_rows)}")

    return len(rows), len(valid_rows), len(quarantine_rows)


# -------------------------------------------------------
# Main
# -------------------------------------------------------
def main():
    print("Starting source validation...")
    print(f"Project root: {get_project_root()}")

    timestamp = get_timestamp()

    try:
        hr_total, hr_valid, hr_quarantine = validate_hr_source(timestamp)
    except Exception as e:
        print(f"  HR validation failed: {e}")
        hr_total = hr_valid = hr_quarantine = 0

    try:
        finance_total, finance_valid, finance_quarantine = validate_finance_source(timestamp)
    except Exception as e:
        print(f"  Finance validation failed: {e}")
        finance_total = finance_valid = finance_quarantine = 0

    try:
        it_total, it_valid, it_quarantine = validate_it_source(timestamp)
    except Exception as e:
        print(f"  IT validation failed: {e}")
        it_total = it_valid = it_quarantine = 0

    print("\nValidation complete.")
    print("Summary:")
    print(f"  HR       — total: {hr_total}, valid: {hr_valid}, quarantined: {hr_quarantine}")
    print(f"  Finance  — total: {finance_total}, valid: {finance_valid}, quarantined: {finance_quarantine}")
    print(f"  IT       — total: {it_total}, valid: {it_valid}, quarantined: {it_quarantine}")


if __name__ == "__main__":
    main()
