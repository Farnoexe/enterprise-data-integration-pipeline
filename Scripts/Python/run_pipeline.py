import subprocess
import sys
from pathlib import Path


# -------------------------------------------------------
# Pipeline runner
# Runs all four pipeline scripts in the correct order:
#   1. ingest_sources.py    — read source files, save raw copies
#   2. validate_sources.py  — validate records, split clean/quarantine
#   3. integrate_datasets.py — join clean sources, produce integrated output
#   4. upload_to_s3.py      — upload all data layers to S3
#
# If any script fails, the pipeline stops immediately.
# This prevents downstream scripts from running on bad data.
# -------------------------------------------------------


def get_scripts_dir():
    # Script lives in Scripts/Python/ — same directory as the other scripts
    return Path(__file__).resolve().parent


def run_script(script_name):
    script_path = get_scripts_dir() / script_name
    print(f"\n{'='*60}")
    print(f"Running: {script_name}")
    print(f"{'='*60}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=False
    )

    if result.returncode != 0:
        print(f"\nPipeline stopped: {script_name} failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def main():
    print("Starting full pipeline run...")

    run_script("ingest_sources.py")
    run_script("validate_sources.py")
    run_script("integrate_datasets.py")
    run_script("upload_to_s3.py")

    print(f"\n{'='*60}")
    print("Pipeline complete. All steps finished successfully.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
