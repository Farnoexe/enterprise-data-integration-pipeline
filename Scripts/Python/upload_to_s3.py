import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pathlib import Path


# -------------------------------------------------------
# Upload script
# Uploads the latest timestamped file from each local
# data layer to the correct S3 prefix.
#
# Uploads:
#   Raw        -> project4-enterprise-integration/raw/
#   Clean      -> project4-enterprise-integration/clean/
#   Quarantine -> project4-enterprise-integration/quarantine/
#   Integrated -> project4-enterprise-integration/integrated/
# -------------------------------------------------------


BUCKET_NAME = "metroville-traffic-analytics"
S3_PREFIX = "project4-enterprise-integration"


def get_project_root():
    # Script lives in Scripts/Python/ so we go up two levels
    return Path(__file__).resolve().parents[2]


def get_latest_file(folder, pattern):
    # Find the latest file based on timestamped filename
    files = sorted(folder.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {folder}")
    return files[-1]


def upload_file(s3_client, local_path, s3_key):
    print(f"  Uploading {local_path.name} -> s3://{BUCKET_NAME}/{s3_key}")
    s3_client.upload_file(str(local_path), BUCKET_NAME, s3_key)
    print("  Upload complete.")


def update_result_counts(result_counts, status):
    if status in result_counts:
        result_counts[status] += 1


def upload_raw_files(s3_client, project_root, result_counts):
    print("Uploading raw files...")

    sources = [
        ("hr", "hr_raw_*.csv"),
        ("finance", "finance_raw_*.csv"),
        ("it", "it_raw_*.csv"),
    ]

    for source_name, pattern in sources:
        try:
            folder = project_root / "Data" / "Raw" / source_name
            latest = get_latest_file(folder, pattern)
            s3_key = f"{S3_PREFIX}/raw/{source_name}/{latest.name}"
            upload_file(s3_client, latest, s3_key)
            update_result_counts(result_counts, "uploaded")
        except FileNotFoundError as e:
            print(f"  Skipping {source_name} raw — {e}")
            update_result_counts(result_counts, "skipped")
        except (BotoCoreError, ClientError) as e:
            print(f"  Failed to upload {source_name} raw — {e}")
            update_result_counts(result_counts, "failed")
        except Exception as e:
            print(f"  Unexpected error uploading {source_name} raw — {e}")
            update_result_counts(result_counts, "failed")


def upload_clean_files(s3_client, project_root, result_counts):
    print("Uploading clean files...")

    sources = [
        ("hr", "hr_clean_*.csv"),
        ("finance", "finance_clean_*.csv"),
        ("it", "it_clean_*.csv"),
    ]

    for source_name, pattern in sources:
        try:
            folder = project_root / "Data" / "Clean" / source_name
            latest = get_latest_file(folder, pattern)
            s3_key = f"{S3_PREFIX}/clean/{source_name}/{latest.name}"
            upload_file(s3_client, latest, s3_key)
            update_result_counts(result_counts, "uploaded")
        except FileNotFoundError as e:
            print(f"  Skipping {source_name} clean — {e}")
            update_result_counts(result_counts, "skipped")
        except (BotoCoreError, ClientError) as e:
            print(f"  Failed to upload {source_name} clean — {e}")
            update_result_counts(result_counts, "failed")
        except Exception as e:
            print(f"  Unexpected error uploading {source_name} clean — {e}")
            update_result_counts(result_counts, "failed")


def upload_quarantine_files(s3_client, project_root, result_counts):
    print("Uploading quarantine files...")

    sources = [
        ("hr", "hr_quarantine_*.csv"),
        ("finance", "finance_quarantine_*.csv"),
        ("it", "it_quarantine_*.csv"),
    ]

    for source_name, pattern in sources:
        try:
            folder = project_root / "Data" / "Quarantine" / source_name
            latest = get_latest_file(folder, pattern)
            s3_key = f"{S3_PREFIX}/quarantine/{source_name}/{latest.name}"
            upload_file(s3_client, latest, s3_key)
            update_result_counts(result_counts, "uploaded")
        except FileNotFoundError as e:
            print(f"  Skipping {source_name} quarantine — {e}")
            update_result_counts(result_counts, "skipped")
        except (BotoCoreError, ClientError) as e:
            print(f"  Failed to upload {source_name} quarantine — {e}")
            update_result_counts(result_counts, "failed")
        except Exception as e:
            print(f"  Unexpected error uploading {source_name} quarantine — {e}")
            update_result_counts(result_counts, "failed")

    try:
        folder = project_root / "Data" / "Quarantine" / "integration"
        latest = get_latest_file(folder, "integration_quarantine_*.csv")
        s3_key = f"{S3_PREFIX}/quarantine/integration/{latest.name}"
        upload_file(s3_client, latest, s3_key)
        update_result_counts(result_counts, "uploaded")
    except FileNotFoundError as e:
        print(f"  Skipping integration quarantine — {e}")
        update_result_counts(result_counts, "skipped")
    except (BotoCoreError, ClientError) as e:
        print(f"  Failed to upload integration quarantine — {e}")
        update_result_counts(result_counts, "failed")
    except Exception as e:
        print(f"  Unexpected error uploading integration quarantine — {e}")
        update_result_counts(result_counts, "failed")


def upload_integrated_file(s3_client, project_root, result_counts):
    print("Uploading integrated output...")

    try:
        folder = project_root / "Data" / "Integrated"
        latest = get_latest_file(folder, "integrated_department_assets_*.csv")
        s3_key = f"{S3_PREFIX}/integrated/{latest.name}"
        upload_file(s3_client, latest, s3_key)
        update_result_counts(result_counts, "uploaded")
    except FileNotFoundError as e:
        print(f"  Skipping integrated output — {e}")
        update_result_counts(result_counts, "skipped")
    except (BotoCoreError, ClientError) as e:
        print(f"  Failed to upload integrated output — {e}")
        update_result_counts(result_counts, "failed")
    except Exception as e:
        print(f"  Unexpected error uploading integrated output — {e}")
        update_result_counts(result_counts, "failed")


def main():
    print("Starting S3 upload...")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Prefix: {S3_PREFIX}")

    project_root = get_project_root()
    result_counts = {
        "uploaded": 0,
        "skipped": 0,
        "failed": 0,
    }

    try:
        s3_client = boto3.client("s3")
    except Exception as e:
        print(f"Failed to create S3 client: {e}")
        return

    upload_raw_files(s3_client, project_root, result_counts)
    upload_clean_files(s3_client, project_root, result_counts)
    upload_quarantine_files(s3_client, project_root, result_counts)
    upload_integrated_file(s3_client, project_root, result_counts)

    print("\nS3 upload finished.")
    print("Summary:")
    print(f"  Files uploaded successfully: {result_counts['uploaded']}")
    print(f"  Files skipped: {result_counts['skipped']}")
    print(f"  Files failed: {result_counts['failed']}")


if __name__ == "__main__":
    main()
