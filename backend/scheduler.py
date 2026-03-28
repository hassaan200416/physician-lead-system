# scheduler.py
# Weekly sync scheduler.
# Runs the NPPES ETL automatically every Sunday at 2:00 AM.
# Usage: python scheduler.py
# Keep this running in the background on your server.

import schedule
import time
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent
ETL_SCRIPT = PROJECT_ROOT / "etl" / "ingest_nppes.py"
NPPES_DIR = PROJECT_ROOT / "raw_data" / "nppes"


def find_latest_nppes_file():
    """
    Returns the most recent NPPES CSV in raw_data/nppes/.

    Sorts lexicographically — relies on the CMS naming convention
    'npidata_pfile_YYYYMMDD-YYYYMMDD.csv' where the end date is
    embedded in the filename, so alphabetical order equals chronological.
    Returns None if no matching file is found.
    """
    csv_files = list(NPPES_DIR.rglob("npidata_pfile*.csv"))
    if csv_files:
        return sorted(csv_files)[-1]
    return None


def run_weekly_sync():
    """Runs the ETL pipeline for the weekly update."""
    print(f"\n{'='*60}")
    print(f"WEEKLY SYNC STARTED: {datetime.now().isoformat()}")
    print(f"{'='*60}")

    csv_path = find_latest_nppes_file()
    if not csv_path:
        print("ERROR: No NPPES file found. Skipping sync.")
        return

    print(f"Processing file: {csv_path.name}")

    result = subprocess.run(
        [sys.executable, str(ETL_SCRIPT), "--file", str(csv_path)],
        capture_output=False,
    )

    if result.returncode == 0:
        print("Weekly sync completed successfully")
    else:
        print(f"Weekly sync failed with return code {result.returncode}")


def run_now():
    """Run sync immediately - useful for manual triggers."""
    run_weekly_sync()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NPPES Sync Scheduler")
    parser.add_argument(
        "--now",
        action="store_true",
        help="Run sync immediately instead of waiting for schedule"
    )
    args = parser.parse_args()

    if args.now:
        run_weekly_sync()
    else:
        print("Scheduler started. Will run every Sunday at 2:00 AM.")
        print("Press Ctrl+C to stop.")

        # Schedule every Sunday at 2:00 AM
        weekly_job: Any = schedule.every().sunday.at("02:00")
        weekly_job.do(run_weekly_sync)

        # Also add to requirements
        print(f"Next run: {schedule.next_run()}")

        while True:
            schedule.run_pending()
            time.sleep(60)
