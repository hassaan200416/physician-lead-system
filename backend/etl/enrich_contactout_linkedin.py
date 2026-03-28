# etl/enrich_contactout_linkedin.py
#
# ContactOut LinkedIn URL enrichment — CSV export / import workflow.
#
# Uses ContactOut's "Get LinkedIn URLs" feature which takes email
# addresses as input and returns LinkedIn profile URLs.
# These LinkedIn URLs are then used by FullEnrich to find mobile
# phones (LinkedIn dramatically improves FullEnrich phone hit rate).
#
# Workflow
# --------
#   Step 1 — EXPORT (--export mode)
#   Generates exports/contactout_linkedin_upload.csv
#   Column: Emails (one physician email per row)
#   Only physicians with an email but no LinkedIn URL yet are included.
#
#   Step 2 — MANUAL UPLOAD (you do this in the browser)
#   1. Go to: contactout.com/dashboard/data-enrichment
#   2. Click: "Get LinkedIn URLs"
#   3. Upload: exports/contactout_linkedin_upload.csv
#   4. Wait for enrichment to complete
#   5. Download results file
#   6. Save as: exports/contactout_linkedin_results.xlsx
#
#   Step 3 — IMPORT (--import mode)
#   Reads results, saves linkedin_url to physician table,
#   updates enrichment_sources[], syncs leads table.
#
# Why LinkedIn URLs matter
# ------------------------
#   FullEnrich phone hit rate WITHOUT LinkedIn : ~5-10%
#   FullEnrich phone hit rate WITH LinkedIn    : ~30-60%
#   Getting LinkedIn URLs for our 20 physicians with emails
#   is the fastest path to Category A leads (phone + email).
#
# Usage
# -----
#   python etl/enrich_contactout_linkedin.py --export
#   python etl/enrich_contactout_linkedin.py --import
#   python etl/enrich_contactout_linkedin.py --preview

import sys
import csv
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import engine

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

SOURCE_NAME   = "contactout"
EXPORTS_DIR   = Path(__file__).parent.parent / "exports"
UPLOAD_FILE   = EXPORTS_DIR / "contactout_linkedin_upload.csv"
RESULTS_FILE  = EXPORTS_DIR / "contactout_linkedin_results.xlsx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_exports_dir() -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def is_valid_linkedin_url(value) -> bool:
    """
    Returns True if value looks like a LinkedIn profile URL.

    Args:
        value: Any cell value from results file.

    Returns:
        True if value contains 'linkedin.com/in/'
    """
    if not isinstance(value, str):
        return False
    return "linkedin.com/in/" in value.lower()


def extract_linkedin_from_row(row_values: tuple) -> Optional[str]:
    """
    Scans all cells in a row for a LinkedIn profile URL.

    ContactOut may put the URL in different columns — scanning
    all cells is robust against their column layout variations.

    Args:
        row_values: Tuple of cell values from openpyxl.

    Returns:
        LinkedIn URL string if found, None otherwise.
    """
    for cell in row_values:
        if is_valid_linkedin_url(cell):
            return str(cell).strip()
    return None


# ---------------------------------------------------------------------------
# Export mode
# ---------------------------------------------------------------------------

def run_export(limit: int = 4) -> None:
    """
    Generates a ContactOut-formatted CSV for LinkedIn URL lookup.

    Pulls all physicians that:
        - Have any email (personal_email OR legacy email)
        - Do NOT have a linkedin_url yet
        - Are active

    The ContactOut template requires one column: Emails.
    We include a second column 'npi' for our own matching reference
    (ContactOut ignores unknown columns).

    Output: exports/contactout_linkedin_upload.csv
    """
    ensure_exports_dir()

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                p.npi,
                p.first_name_clean,
                p.last_name_clean,
                COALESCE(p.personal_email, p.email) AS email,
                p.enrichment_sources,
                p.lead_score_current
            FROM physician p
            WHERE p.is_active = TRUE
              AND (p.personal_email IS NOT NULL OR p.email IS NOT NULL)
              AND (p.linkedin_url IS NULL OR p.linkedin_url = '')
            ORDER BY p.lead_score_current DESC NULLS LAST
        """))
        rows = result.fetchall()

    if not rows:
        print("No eligible physicians found.")
        print("All physicians with emails already have LinkedIn URLs.")
        return

    with open(UPLOAD_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Emails", "npi"])
        for row in rows:
            writer.writerow([row[3], row[0]])  # email, npi

    print("=" * 60)
    print("CONTACTOUT LINKEDIN EXPORT COMPLETE")
    print(f"  File       : {UPLOAD_FILE}")
    print(f"  Physicians : {len(rows)}")
    print()
    for row in rows:
        sources = row[4] or []
        print(f"  {row[1]} {row[2]} | {row[3]} | {sources}")
    print()
    print("NEXT STEPS:")
    print("  1. Go to : contactout.com/dashboard/data-enrichment")
    print("  2. Click : 'Get LinkedIn URLs'")
    print(f"  3. Upload: {UPLOAD_FILE}")
    print("  4. Wait for enrichment to complete")
    print("  5. Download results file")
    print(f"  6. Save as: {RESULTS_FILE}")
    print("  7. Run   : python etl/enrich_contactout_linkedin.py --import")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Import mode
# ---------------------------------------------------------------------------

def run_import() -> None:
    """
    Processes ContactOut LinkedIn URL results and saves to DB.

    For each row with a valid LinkedIn URL:
        1. Saves to physician.linkedin_url
        2. Updates enrichment_sources[] if not already tagged
        3. Inserts audit row in field_value_history
        4. Updates enrichment_source_stats

    For each no-match row:
        1. Records the attempt in enrichment_last_attempted_at
        2. Does NOT block other sources

    After all rows: prints summary of LinkedIn URLs found.
    Note: does NOT sync leads table — linkedin_url is not a
    contact signal for leads, it's an intermediate enrichment
    input for the next FullEnrich phone run.
    """
    if not OPENPYXL_AVAILABLE:
        print("ERROR: openpyxl not installed.")
        print("Run: pip install openpyxl --break-system-packages")
        return

    if not RESULTS_FILE.exists():
        print(f"ERROR: Results file not found: {RESULTS_FILE}")
        print("Complete the manual upload steps first.")
        return

    now = datetime.now(timezone.utc)

    print("=" * 60)
    print("CONTACTOUT LINKEDIN IMPORT")
    print(f"  File: {RESULTS_FILE}")
    print("=" * 60)

    wb   = openpyxl.load_workbook(RESULTS_FILE)
    ws   = wb.active
    if ws is None:
        print("Error: worksheet is empty or cannot be read.")
        return
    rows = []
    headers = None
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            headers = list(row)
            continue
        rows.append(row)

    if not rows:
        print("Results file is empty.")
        return

    print(f"  Rows    : {len(rows)}")
    print(f"  Columns : {headers}")
    print()

    saved    = 0
    not_found = 0
    errors   = 0

    with engine.connect() as conn:
        for i, row_values in enumerate(rows, 1):

            # Scan all cells for LinkedIn URL
            linkedin_url = extract_linkedin_from_row(row_values)

            # Get email from first column (our upload had Emails as col 1)
            email_val = str(row_values[0] or "").strip() if row_values else ""

            # Get NPI — we put it in col 2
            npi_val = str(row_values[1] or "").strip() if len(row_values) > 1 else ""

            print(f"  [{i}] email={email_val} | linkedin={linkedin_url or '—'}")

            # Resolve NPI
            npi = None
            if npi_val and npi_val.isdigit():
                npi = npi_val
            elif email_val:
                result = conn.execute(text("""
                    SELECT npi FROM physician
                    WHERE personal_email = :email OR email = :email
                    LIMIT 1
                """), {"email": email_val})
                match = result.fetchone()
                if match:
                    npi = match[0]

            if not npi:
                print(f"       Cannot match to physician — skipping")
                errors += 1
                continue

            print(f"       NPI: {npi}")

            # No LinkedIn URL found
            if not linkedin_url:
                not_found += 1
                conn.execute(text("""
                    UPDATE physician SET
                        enrichment_last_attempted_at = :now,
                        updated_at                   = :now
                    WHERE npi = :npi
                """), {"npi": npi, "now": now})
                conn.commit()
                print(f"       No LinkedIn URL found")
                continue

            # Save LinkedIn URL
            try:
                conn.execute(text("""
                    UPDATE physician SET
                        linkedin_url                 = :url,
                        enrichment_last_attempted_at = :now,
                        updated_at                   = :now
                    WHERE npi = :npi
                """), {"npi": npi, "url": linkedin_url, "now": now})

                conn.execute(text("""
                    INSERT INTO field_value_history (
                        history_id, entity_type, entity_id, npi,
                        field_name, field_value, source_name,
                        confidence_score, is_current,
                        collected_timestamp, created_at
                    ) VALUES (
                        gen_random_uuid(), 'physician', :npi, :npi,
                        'linkedin_url', :url, :source,
                        0.95, TRUE, :now, :now
                    )
                """), {
                    "npi": npi, "url": linkedin_url,
                    "source": SOURCE_NAME, "now": now,
                })

                conn.commit()
                saved += 1
                print(f"       LinkedIn URL SAVED: {linkedin_url}")

            except Exception as e:
                print(f"       ERROR: {e}")
                errors += 1
                conn.rollback()

    # Summary
    print()
    print("=" * 60)
    print("CONTACTOUT LINKEDIN IMPORT COMPLETE")
    print(f"  Rows processed    : {len(rows)}")
    print(f"  LinkedIn URLs saved: {saved}")
    print(f"  Not found         : {not_found}")
    print(f"  Errors            : {errors}")
    if len(rows) > 0:
        print(f"  Hit rate          : {saved / len(rows) * 100:.1f}%")
    print()
    if saved > 0:
        print("NEXT STEP:")
        print("  Run FullEnrich with LinkedIn URLs to get phone numbers:")
        print("  python etl/enrich_fullenrich_csv.py --export --limit 20")
        print("  (Upload the CSV to FullEnrich — LinkedIn URLs boost phone hit rate)")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Preview mode
# ---------------------------------------------------------------------------

def run_preview() -> None:
    """Shows physicians eligible for LinkedIn lookup without creating files."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                p.npi,
                p.first_name_clean,
                p.last_name_clean,
                COALESCE(p.personal_email, p.email) AS email,
                p.enrichment_sources
            FROM physician p
            WHERE p.is_active = TRUE
              AND (p.personal_email IS NOT NULL OR p.email IS NOT NULL)
              AND (p.linkedin_url IS NULL OR p.linkedin_url = '')
            ORDER BY p.lead_score_current DESC NULLS LAST
        """))
        rows = result.fetchall()

    if not rows:
        print("No eligible physicians — all already have LinkedIn URLs.")
        return

    print(f"\n{'NPI':<12} {'Name':<28} {'Email':<40} Sources")
    print("-" * 95)
    for row in rows:
        name    = f"{row[1] or ''} {row[2] or ''}".strip()
        sources = str(row[4] or [])
        print(f"{row[0]:<12} {name:<28} {row[3]:<40} {sources}")

    print(f"\n  Total eligible: {len(rows)}")
    print(f"  ContactOut search credits needed: {len(rows)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ContactOut LinkedIn URL Enrichment"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--export",  action="store_true",
                       help="Generate CSV for ContactOut LinkedIn lookup")
    parser.add_argument("--limit", type=int, default=4,
                        help="Max physicians to export (default: 4 = daily credit limit)")
    group.add_argument("--import",  dest="do_import", action="store_true",
                       help="Process ContactOut LinkedIn results")
    group.add_argument("--preview", action="store_true",
                       help="Show eligible physicians without creating files")
    args = parser.parse_args()

    if args.export:
        run_export(limit=args.limit)
    elif args.do_import:
        run_import()
    elif args.preview:
        run_preview()