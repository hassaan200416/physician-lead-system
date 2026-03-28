# etl/enrich_fullenrich_csv.py
#
# FullEnrich enrichment — CSV upload / download workflow.
#
# FullEnrich's free tier only works through the dashboard CSV upload.
# The API requires a paid plan. This script handles both sides of the
# manual workflow.
#
# Credit costs (confirmed from dashboard)
# ----------------------------------------
#   Mobile phone   : 10 credits per number found
#   Work email     : 1 credit per email found
#   Personal email : 3 credits per email found
#   Full contact   : 14 credits if all three are found
#   NO charge if nothing is found for a contact.
#
# Workflow
# --------
#   Step 1 — EXPORT (--export mode)
#   Generates exports/fullenrich_upload.csv with columns:
#     First Name | Last Name | Website | LinkedIn Profile URL | npi
#   LinkedIn URL is blank (we don't have it).
#   Website = practice_domain if available, else blank.
#   npi column is extra — FullEnrich ignores unknown columns,
#   we use it to match results back without ambiguity.
#
#   Step 2 — MANUAL UPLOAD (you do this in the browser)
#   1. Go to: app.fullenrich.com/app/enrich
#   2. Click: "Enrich CSV / Excel"
#   3. Select: Mobile phone + Work Email + Personal Email (all three)
#   4. Upload: exports/fullenrich_upload.csv
#   5. Wait for enrichment to complete
#   6. Click Download on the completed batch
#   7. Save file as: exports/fullenrich_results.csv
#
#   Step 3 — IMPORT (--import mode)
#   Reads exports/fullenrich_results.csv, saves phone + emails to DB,
#   updates enrichment_sources[], updates stats, syncs leads table.
#
# Usage
# -----
#   python etl/enrich_fullenrich_csv.py --export --limit 2
#   python etl/enrich_fullenrich_csv.py --import
#   python etl/enrich_fullenrich_csv.py --preview --limit 10
#
# Credit guidance
# ---------------
#   With 35 credits: --limit 2 (14 credits max if both contacts hit)
#   After top-up: increase limit accordingly
#   Always check credits on dashboard before uploading

import sys
import csv
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import engine

SOURCE_NAME  = "fullenrich"
EXPORTS_DIR  = Path(__file__).parent.parent / "exports"
UPLOAD_FILE  = EXPORTS_DIR / "fullenrich_upload.csv"
RESULTS_FILE = EXPORTS_DIR / "fullenrich_results.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_exports_dir() -> None:
    """Creates the exports/ directory if it does not exist."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def is_valid_email(value) -> bool:
    """
    Returns True if value looks like a valid email address.

    Args:
        value: Any cell value from the results CSV.

    Returns:
        True if value is a non-empty string containing '@'.
    """
    if not isinstance(value, str):
        return False
    value = value.strip()
    return "@" in value and "." in value.split("@")[-1]


def is_valid_phone(value) -> bool:
    """
    Returns True if value looks like a phone number.

    FullEnrich returns phone numbers in E.164 format (+1XXXXXXXXXX)
    or local format. We check for digit density.

    Args:
        value: Any cell value from the results CSV.

    Returns:
        True if value has 7+ digits (likely a phone number).
    """
    if not isinstance(value, str):
        return False
    digits = [c for c in value if c.isdigit()]
    return len(digits) >= 7


# ---------------------------------------------------------------------------
# Export mode
# ---------------------------------------------------------------------------

def run_export(limit: int = 2) -> None:
    """
    Generates a FullEnrich-formatted CSV for manual dashboard upload.

    Pulls physicians that:
        - FullEnrich has not already tried (not in enrichment_sources[])
        - Are missing phone OR email (not fully enriched)
        - Are active
        - Have a domain or org name (FullEnrich needs at least one)

    Ordered by:
        1. Physicians with domains first (higher match rate)
        2. Lead score DESC within each group

    Output: exports/fullenrich_upload.csv
    Columns: First Name, Last Name, Website, LinkedIn Profile URL, npi

    The 'npi' and 'LinkedIn Profile URL' columns:
        - npi: our internal reference, FullEnrich ignores it
        - LinkedIn Profile URL: left blank, improves match rate if added later

    Args:
        limit: Max physicians to export. Keep low to match credit budget.
               Default 2 = safe for 35 remaining credits.
    """
    ensure_exports_dir()

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                p.npi,
                p.first_name_clean,
                p.last_name_clean,
                p.practice_domain,
                p.organization_name,
                p.lead_score_current,
                p.linkedin_url
            FROM physician p
            WHERE p.is_active = TRUE
              AND NOT (p.enrichment_sources @> ARRAY['fullenrich']::text[])
              AND NOT (
                p.mobile_phone IS NOT NULL
                AND (p.personal_email IS NOT NULL OR p.email IS NOT NULL)
              )
              AND (p.practice_domain IS NOT NULL OR p.organization_name IS NOT NULL)
            ORDER BY
                -- Physicians with LinkedIn URLs first (much higher phone hit rate)
                CASE WHEN p.linkedin_url IS NOT NULL THEN 0 ELSE 1 END ASC,
                CASE WHEN p.practice_domain IS NOT NULL THEN 0 ELSE 1 END ASC,
                p.lead_score_current DESC NULLS LAST
            LIMIT :limit
        """), {"limit": limit})
        rows = result.fetchall()

    if not rows:
        print("No eligible physicians found for FullEnrich export.")
        return

    with open(UPLOAD_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # FullEnrich template columns + npi for import matching
        writer.writerow([
            "First Name", "Last Name", "Website",
            "LinkedIn Profile URL", "npi"
        ])
        for row in rows:
            npi          = row[0]
            first        = row[1] or ""
            last         = row[2] or ""
            domain       = row[3] or ""
            linkedin_url = row[6] or ""  # Include LinkedIn URL if available
            writer.writerow([first, last, domain, linkedin_url, npi])

    print("=" * 60)
    print("FULLENRICH CSV EXPORT COMPLETE")
    print(f"  File       : {UPLOAD_FILE}")
    print(f"  Physicians : {len(rows)}")
    print()
    for row in rows:
        print(f"  {row[1]} {row[2]} | {row[3] or row[4]} | score {row[5]}")
    print()
    print(f"  Max credit cost : {len(rows)} × 14 = {len(rows) * 14} credits")
    print(f"  (Only charged if data is found — no hit = no charge)")
    print()
    print("NEXT STEPS:")
    print("  1. Go to  : app.fullenrich.com/app/enrich")
    print("  2. Click  : 'Enrich CSV / Excel'")
    print("  3. Select : Mobile phone + Work Email + Personal Email")
    print(f"  4. Upload : {UPLOAD_FILE}")
    print("  5. Wait for enrichment to complete")
    print("  6. Click Download on the completed batch")
    print(f"  7. Save as: {RESULTS_FILE}")
    print("  8. Run    : python etl/enrich_fullenrich_csv.py --import")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Preview mode
# ---------------------------------------------------------------------------

def run_preview(limit: int) -> None:
    """
    Shows which physicians would be exported without creating any files.

    Args:
        limit: Max physicians to show.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                p.npi,
                p.first_name_clean,
                p.last_name_clean,
                p.practice_domain,
                p.organization_name,
                p.lead_score_current,
                p.linkedin_url
            FROM physician p
            WHERE p.is_active = TRUE
              AND NOT (p.enrichment_sources @> ARRAY['fullenrich']::text[])
              AND NOT (
                p.mobile_phone IS NOT NULL
                AND (p.personal_email IS NOT NULL OR p.email IS NOT NULL)
              )
              AND (p.practice_domain IS NOT NULL OR p.organization_name IS NOT NULL)
            ORDER BY
                -- Physicians with LinkedIn URLs first (much higher phone hit rate)
                CASE WHEN p.linkedin_url IS NOT NULL THEN 0 ELSE 1 END ASC,
                CASE WHEN p.practice_domain IS NOT NULL THEN 0 ELSE 1 END ASC,
                p.lead_score_current DESC NULLS LAST
            LIMIT :limit
        """), {"limit": limit})
        rows = result.fetchall()

    if not rows:
        print("No eligible physicians found.")
        return

    print(f"\n{'NPI':<12} {'Name':<28} {'Domain/Org':<38} Score")
    print("-" * 85)
    for row in rows:
        name    = f"{row[1] or ''} {row[2] or ''}".strip()
        context = row[3] or row[4] or "—"
        print(f"{row[0]:<12} {name:<28} {context:<38} {row[5]}")

    print(f"\n  Total eligible : {len(rows)}")
    print(f"  Max credits    : {len(rows)} × 14 = {len(rows) * 14}")
    print(f"  (No charge for contacts with no data found)")


# ---------------------------------------------------------------------------
# Import mode
# ---------------------------------------------------------------------------

def detect_columns(headers: list[str]) -> dict:
    """
    Detects which columns contain phone, work email, and personal email
    in the FullEnrich results CSV.

    FullEnrich may vary column names between versions. This scans
    headers for known patterns rather than hardcoding positions.

    Args:
        headers: List of column name strings from CSV header row.

    Returns:
        Dict with keys: npi, phone, work_email, personal_email, first, last
        Values are column name strings or None if not found.
    """
    lower = {h.lower().strip(): h for h in headers}

    def find(*candidates) -> Optional[str]:
        for c in candidates:
            if c.lower() in lower:
                return lower[c.lower()]
        return None

    return {
        "npi":            find("npi"),
        "first":          find("first name", "firstname", "first_name"),
        "last":           find("last name",  "lastname",  "last_name"),
        # FullEnrich CSV export uses these exact column names
        "phone":          find("phone number (fullenrich)", "phone",
                               "mobile phone", "mobile_phone", "phone number",
                               "mobile", "all mobile phone numbers (fullenrich)"),
        "work_email":     find("email (fullenrich)", "work email",
                               "work_email", "workemail", "email",
                               "professional email"),
        "personal_email": find("personal email (fullenrich)", "personal email",
                               "personal_email", "personalemail", "private email"),
    }


def resolve_npi(npi_val: str, first: str, last: str, conn) -> Optional[str]:
    """
    Resolves physician NPI from results row.

    Primary: use npi column we included in the upload.
    Fallback: name match if npi column is missing or empty.

    Args:
        npi_val: Value from npi column (may be empty).
        first:   First name from results.
        last:    Last name from results.
        conn:    Active DB connection.

    Returns:
        NPI string or None.
    """
    if npi_val and npi_val.strip():
        return npi_val.strip()

    if first and last:
        result = conn.execute(text("""
            SELECT npi FROM physician
            WHERE LOWER(first_name_clean) = LOWER(:first)
              AND LOWER(last_name_clean)  = LOWER(:last)
            LIMIT 1
        """), {"first": first.strip(), "last": last.strip()})
        row = result.fetchone()
        if row:
            return row[0]

    return None


def run_import() -> None:
    """
    Processes the FullEnrich results CSV and saves contact data to DB.

    For each row with phone or email data:
        1. Saves mobile_phone if present
        2. Saves personal_email (prefers personal over work email)
        3. Appends 'fullenrich' to physician.enrichment_sources[]
        4. Inserts audit rows in field_value_history
        5. Updates enrichment_source_stats atomically
        6. Syncs leads table with updated contact_category

    For each no-data row:
        1. Appends 'fullenrich' to enrichment_sources[] (per-source guard)
        2. Increments total_attempts in stats
        3. Does NOT set email_enrichment_attempted — other sources can retry

    contact_category after import:
        A = mobile_phone + any email → ready for outreach
        B = any email, no phone     → email outreach only
    """
    if not RESULTS_FILE.exists():
        print(f"ERROR: Results file not found: {RESULTS_FILE}")
        print("Complete the manual upload steps first, then re-run --import.")
        return

    now = datetime.now(timezone.utc)

    print("=" * 60)
    print("FULLENRICH CSV IMPORT")
    print(f"  File: {RESULTS_FILE}")
    print("=" * 60)

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        reader  = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows    = list(reader)

    if not rows:
        print("Results file is empty.")
        return

    print(f"  Rows     : {len(rows)}")
    print(f"  Columns  : {headers}")

    cols = detect_columns(headers)
    print(f"  Detected : {cols}")
    print()

    saved_emails = 0
    saved_phones = 0
    not_found    = 0
    errors       = 0

    with engine.connect() as conn:
        for i, row in enumerate(rows, 1):

            # ── Read values ────────────────────────────────────────────
            npi_raw   = str(row.get(cols["npi"]            or "", "") or "").strip()
            first     = str(row.get(cols["first"]          or "", "") or "").strip()
            last      = str(row.get(cols["last"]           or "", "") or "").strip()
            phone_raw = str(row.get(cols["phone"]          or "", "") or "").strip()
            work_raw  = str(row.get(cols["work_email"]     or "", "") or "").strip()
            pers_raw  = str(row.get(cols["personal_email"] or "", "") or "").strip()

            # ── Resolve NPI ────────────────────────────────────────────
            npi = resolve_npi(npi_raw, first, last, conn)
            if not npi:
                print(f"  [{i}] Cannot match to physician — skipping")
                errors += 1
                continue

            # ── Extract valid contact data ─────────────────────────────
            phone        = phone_raw if is_valid_phone(phone_raw) else ""
            work_email   = work_raw  if is_valid_email(work_raw)  else ""
            pers_email   = pers_raw  if is_valid_email(pers_raw)  else ""
            best_email   = pers_email or work_email

            has_phone = bool(phone)
            has_email = bool(best_email)

            print(f"  [{i}] {first} {last} | NPI {npi}")
            print(f"       phone={phone or '—'} | email={best_email or '—'}")

            # ── No data found ──────────────────────────────────────────
            if not has_phone and not has_email:
                not_found += 1
                # Per-source guard — append to array, don't block others
                conn.execute(text("""
                    UPDATE physician SET
                        enrichment_sources = CASE
                            WHEN enrichment_sources @> ARRAY['fullenrich']::text[]
                            THEN enrichment_sources
                            ELSE array_append(enrichment_sources, 'fullenrich')
                        END,
                        enrichment_last_attempted_at = :now,
                        updated_at                   = :now
                    WHERE npi = :npi
                """), {"npi": npi, "now": now})
                conn.execute(text("""
                    UPDATE enrichment_source_stats SET
                        total_attempts = total_attempts + 1,
                        last_used_at   = :now,
                        updated_at     = :now
                    WHERE source_name  = :source
                """), {"source": SOURCE_NAME, "now": now})
                conn.commit()
                print(f"       No data — marked as attempted by fullenrich")
                continue

            # ── Save contact data ──────────────────────────────────────
            try:
                conn.execute(text("""
                    UPDATE physician SET
                        personal_email = CASE
                            WHEN :email != '' THEN :email
                            ELSE personal_email
                        END,
                        personal_email_confidence = CASE
                            WHEN :email != '' THEN 'HIGH'
                            ELSE personal_email_confidence
                        END,
                        email_enriched_at = CASE
                            WHEN :email != '' THEN :now
                            ELSE email_enriched_at
                        END,
                        mobile_phone = CASE
                            WHEN :phone != '' THEN :phone
                            ELSE mobile_phone
                        END,
                        phone_confidence = CASE
                            WHEN :phone != '' THEN 'HIGH'
                            ELSE phone_confidence
                        END,
                        phone_enriched_at = CASE
                            WHEN :phone != '' THEN :now
                            ELSE phone_enriched_at
                        END,
                        enrichment_sources = CASE
                            WHEN enrichment_sources @> ARRAY['fullenrich']::text[]
                            THEN enrichment_sources
                            ELSE array_append(enrichment_sources, 'fullenrich')
                        END,
                        enrichment_last_attempted_at = :now,
                        updated_at                   = :now
                    WHERE npi = :npi
                """), {
                    "npi":   npi,
                    "email": best_email,
                    "phone": phone,
                    "now":   now,
                })

                # Audit trail — email
                if has_email:
                    conn.execute(text("""
                        INSERT INTO field_value_history (
                            history_id, entity_type, entity_id, npi,
                            field_name, field_value, source_name,
                            confidence_score, is_current,
                            collected_timestamp, created_at
                        ) VALUES (
                            gen_random_uuid(), 'physician', :npi, :npi,
                            'personal_email', :email, :source,
                            0.90, TRUE, :now, :now
                        )
                    """), {
                        "npi": npi, "email": best_email,
                        "source": SOURCE_NAME, "now": now,
                    })

                # Audit trail — phone
                if has_phone:
                    conn.execute(text("""
                        INSERT INTO field_value_history (
                            history_id, entity_type, entity_id, npi,
                            field_name, field_value, source_name,
                            confidence_score, is_current,
                            collected_timestamp, created_at
                        ) VALUES (
                            gen_random_uuid(), 'physician', :npi, :npi,
                            'mobile_phone', :phone, :source,
                            0.90, TRUE, :now, :now
                        )
                    """), {
                        "npi": npi, "phone": phone,
                        "source": SOURCE_NAME, "now": now,
                    })

                # Stats — atomic increment
                conn.execute(text("""
                    UPDATE enrichment_source_stats SET
                        emails_provided = emails_provided + :emails,
                        phones_provided = phones_provided + :phones,
                        total_hits      = total_hits      + 1,
                        total_attempts  = total_attempts  + 1,
                        last_used_at    = :now,
                        updated_at      = :now
                    WHERE source_name   = :source
                """), {
                    "emails": 1 if has_email else 0,
                    "phones": 1 if has_phone else 0,
                    "source": SOURCE_NAME,
                    "now":    now,
                })

                conn.commit()

                if has_email:
                    saved_emails += 1
                    print(f"       Email SAVED: {best_email}")
                if has_phone:
                    saved_phones += 1
                    print(f"       Phone SAVED: {phone}")

            except Exception as e:
                print(f"       ERROR: {e}")
                errors += 1
                conn.rollback()

    # ── Sync leads table ───────────────────────────────────────────────
    _sync_leads(now)

    # ── Summary ────────────────────────────────────────────────────────
    total = len(rows)
    print()
    print("=" * 60)
    print("FULLENRICH CSV IMPORT COMPLETE")
    print(f"  Rows processed : {total}")
    print(f"  Emails saved   : {saved_emails}")
    print(f"  Phones saved   : {saved_phones}")
    print(f"  No data found  : {not_found}")
    print(f"  Errors         : {errors}")
    if total > 0:
        print(f"  Email hit rate : {saved_emails / total * 100:.1f}%")
        print(f"  Phone hit rate : {saved_phones / total * 100:.1f}%")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Leads sync
# ---------------------------------------------------------------------------

def _sync_leads(now: datetime) -> None:
    """
    Syncs physicians with contact data into the leads table.

    contact_category:
        A = mobile_phone + any email
        B = any email, no mobile_phone
        Rows with no contact info never inserted.

    Idempotent — safe to call multiple times.
    """
    print("\nSyncing leads table...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO leads (
                npi, first_name, last_name, full_name, credential,
                specialty, specialty_category, organization_name,
                practice_domain,
                email, personal_email, personal_email_confidence,
                practice_email,
                email_confidence_score, email_confidence_level,
                email_verification_status, email_source,
                address_line_1, city, state, zip,
                lead_score, lead_tier,
                years_of_experience, experience_bucket,
                license_count, multi_state_flag,
                mobile_phone, phone_confidence,
                contact_completeness,
                contact_category,
                enrichment_sources,
                created_at, updated_at
            )
            SELECT
                p.npi, p.first_name_clean, p.last_name_clean,
                p.full_name_display, p.credential_normalized,
                p.specialty_name, p.derived_specialty_category,
                p.organization_name, p.practice_domain,
                p.email, p.personal_email, p.personal_email_confidence,
                p.practice_email,
                p.email_confidence_score, p.email_confidence_level,
                p.email_verification_status, p.email_source,
                ppl.address_line_1, ppl.city, ppl.state, ppl.zip,
                p.lead_score_current, p.lead_tier,
                p.years_of_experience, p.experience_bucket,
                p.license_count, p.multi_state_flag,
                p.mobile_phone, p.phone_confidence,
                p.contact_completeness,
                CASE
                    WHEN p.mobile_phone IS NOT NULL
                     AND (
                           p.personal_email IS NOT NULL
                        OR p.practice_email IS NOT NULL
                        OR p.email          IS NOT NULL
                     )
                    THEN 'A'
                    WHEN (
                           p.personal_email IS NOT NULL
                        OR p.practice_email IS NOT NULL
                        OR p.email          IS NOT NULL
                    )
                    THEN 'B'
                    ELSE NULL
                END,
                p.enrichment_sources,
                :now, :now
            FROM physician p
            LEFT JOIN physician_practice_locations ppl
                ON p.npi = ppl.npi
               AND ppl.is_primary_location = TRUE
            WHERE (
                p.personal_email IS NOT NULL
                OR p.practice_email IS NOT NULL
                OR p.email IS NOT NULL
            )
            ON CONFLICT (npi) DO UPDATE SET
                personal_email            = EXCLUDED.personal_email,
                personal_email_confidence = EXCLUDED.personal_email_confidence,
                email                     = EXCLUDED.email,
                email_confidence_score    = EXCLUDED.email_confidence_score,
                email_confidence_level    = EXCLUDED.email_confidence_level,
                email_verification_status = EXCLUDED.email_verification_status,
                mobile_phone              = EXCLUDED.mobile_phone,
                phone_confidence          = EXCLUDED.phone_confidence,
                lead_score                = EXCLUDED.lead_score,
                lead_tier                 = EXCLUDED.lead_tier,
                contact_category          = EXCLUDED.contact_category,
                enrichment_sources        = EXCLUDED.enrichment_sources,
                updated_at                = EXCLUDED.updated_at
        """), {"now": now})
        conn.commit()

        result = conn.execute(text("SELECT COUNT(*) FROM leads"))
        count  = int(result.fetchone()[0])
        print(f"  Total leads: {count}")

        cats = conn.execute(text("""
            SELECT contact_category, COUNT(*) AS cnt
            FROM leads GROUP BY contact_category ORDER BY contact_category
        """))
        for r in cats.fetchall():
            print(f"  Category {r[0]}: {r[1]}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FullEnrich CSV Export / Import — Phone + Email Enrichment"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--export", action="store_true",
        help="Generate CSV for FullEnrich dashboard upload"
    )
    group.add_argument(
        "--import", dest="do_import", action="store_true",
        help="Process FullEnrich results CSV"
    )
    group.add_argument(
        "--preview", action="store_true",
        help="Show who would be exported without creating files"
    )
    parser.add_argument(
        "--limit", type=int, default=2,
        help="Max physicians to export (default: 2 = safe for 35 credits)"
    )
    args = parser.parse_args()

    if args.export:
        run_export(limit=args.limit)
    elif args.do_import:
        run_import()
    elif args.preview:
        run_preview(limit=args.limit)