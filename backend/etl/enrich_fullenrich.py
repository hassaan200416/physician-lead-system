# etl/enrich_fullenrich.py
#
# FullEnrich enrichment pipeline — phone + work email + personal email.
#
# FullEnrich is a waterfall tool that queries 20+ data sources internally.
# It is our primary source for mobile phone numbers.
#
# API flow (async — not real-time)
# ---------------------------------
#   1. POST /contact/enrich/bulk  → submit batch → get enrichment_id
#   2. Poll GET /contact/enrich/bulk/{id} every 10s until status = 'done'
#   3. Parse results → save phone + emails to DB
#
# Credit costs per contact
# ------------------------
#   Work email    :  1 credit
#   Personal email:  3 credits
#   Mobile phone  : 10 credits
#   Full contact  : 14 credits total
#
# With 36 credits remaining:
#   Full (phone+both emails): 36 ÷ 14 = 2 contacts max
#   Email only (work+personal): 36 ÷ 4 = 9 contacts max
#
# This script runs in two modes based on --mode flag:
#   full   — phone + work email + personal email (14 credits each)
#             Use for top-scored physicians only
#   email  — work email + personal email only (4 credits each)
#             Use for remaining physicians after full mode
#
# Input accepted by FullEnrich API
# ----------------------------------
#   firstname + lastname + domain        (best match rate)
#   firstname + lastname + company_name  (good match rate)
#   linkedin_url                         (best overall but we don't have these)
#
# Usage
# -----
#   python etl/enrich_fullenrich.py --mode full  --limit 2
#   python etl/enrich_fullenrich.py --mode email --limit 9
#   python etl/enrich_fullenrich.py --mode full  --npi 1234567890
#   python etl/enrich_fullenrich.py --preview    # shows who would be processed
#
# Environment variables required (.env)
# --------------------------------------
#   FULLENRICH_API_KEY

import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import engine
from dotenv import load_dotenv
import os

load_dotenv()

FULLENRICH_API_KEY = os.getenv("FULLENRICH_API_KEY")
FULLENRICH_BASE    = "https://app.fullenrich.com/api/v1"
SOURCE_NAME        = "fullenrich"

POLL_INTERVAL_SECONDS = 10   # how often to check if enrichment is done
POLL_MAX_ATTEMPTS     = 30   # give up after 5 minutes (30 × 10s)


# ---------------------------------------------------------------------------
# Credit field mapping
# ---------------------------------------------------------------------------

def get_enrich_fields(mode: str) -> list[str]:
    """
    Returns the FullEnrich field keys to request based on mode.

    FullEnrich only charges credits for fields you explicitly request.
    Requesting fewer fields = fewer credits spent.

    Modes:
        full  — phone (10) + work email (1) + personal email (3) = 14 credits
        email — work email (1) + personal email (3) = 4 credits

    Args:
        mode: 'full' or 'email'

    Returns:
        List of FullEnrich enrich_fields strings.
    """
    if mode == "full":
        return ["contact.phones", "contact.work_emails", "contact.personal_emails"]
    return ["contact.work_emails", "contact.personal_emails"]


# ---------------------------------------------------------------------------
# Step 1 — Pull physicians
# ---------------------------------------------------------------------------

def get_physicians_to_enrich(
    mode:  str,
    limit: Optional[int] = None,
    npi:   Optional[str] = None,
) -> list:
    """
    Fetches physicians eligible for FullEnrich enrichment.

    Eligibility:
        - No personal_email AND no legacy email yet
        - Not already attempted by email enrichment
        - Active
        - Has organization_name OR practice_domain (FullEnrich needs one)

    For 'full' mode (phone enrichment), also excludes physicians that
    already have a mobile_phone from a previous run.

    Ordered by lead_score_current DESC — highest value leads first.

    Args:
        mode:  'full' (phone+email) or 'email' (email only).
        limit: Max records to return.
        npi:   Single NPI override for testing.

    Returns:
        List of row tuples:
        (npi, first_name, last_name, organization_name, practice_domain,
         lead_score_current, lead_tier)
    """
    with engine.connect() as conn:
        if npi:
            result = conn.execute(text("""
                SELECT npi, first_name_clean, last_name_clean,
                       organization_name, practice_domain,
                       lead_score_current, lead_tier
                FROM physician
                WHERE npi = :npi
            """), {"npi": npi})
            rows = result.fetchall()
            print(f"NPI mode: found {len(rows)} physician")
            return list(rows)

        # Per-source eligibility:
        # - Skip if fullenrich already tried this physician (in enrichment_sources[])
        # - Skip if physician already has BOTH email AND phone (fully enriched)
        # - For full mode: also process physicians missing a phone even if they have email
        # - For email mode: skip if personal_email already found

        if mode == "full":
            # Need phone OR email missing — try anyone FullEnrich hasn't seen yet
            contact_filter = """
              AND NOT (enrichment_sources @> ARRAY['fullenrich']::text[])
              AND NOT (p.mobile_phone IS NOT NULL
                       AND (p.personal_email IS NOT NULL OR p.email IS NOT NULL))
            """
        else:
            # Email only mode — skip if already have an email
            contact_filter = """
              AND NOT (enrichment_sources @> ARRAY['fullenrich']::text[])
              AND p.personal_email IS NULL
              AND p.email IS NULL
            """

        query = f"""
            SELECT p.npi, p.first_name_clean, p.last_name_clean,
                   p.organization_name, p.practice_domain,
                   p.lead_score_current, p.lead_tier
            FROM physician p
            WHERE p.is_active = TRUE
              AND (p.organization_name IS NOT NULL OR p.practice_domain IS NOT NULL)
              {contact_filter}
            ORDER BY
                -- Physicians with domains first (higher match rate on FullEnrich)
                CASE WHEN p.practice_domain IS NOT NULL THEN 0 ELSE 1 END ASC,
                p.lead_score_current DESC NULLS LAST
        """
        if limit:
            query += f" LIMIT {limit}"

        result = conn.execute(text(query))
        rows   = result.fetchall()
        print(f"Found {len(rows)} physicians eligible for FullEnrich ({mode} mode)")
        return list(rows)


# ---------------------------------------------------------------------------
# Step 2 — Submit batch to FullEnrich API
# ---------------------------------------------------------------------------

def submit_enrichment_batch(
    physicians: list,
    mode: str,
) -> Optional[str]:
    """
    Submits a batch of physicians to the FullEnrich bulk enrichment API.

    Builds the request payload from physician rows. Uses domain if
    available, falls back to company_name. Both are valid inputs.

    Args:
        physicians: List of physician row tuples from get_physicians_to_enrich.
        mode:       'full' or 'email' — determines which fields to request.

    Returns:
        enrichment_id string if submission succeeded, None if failed.
    """
    enrich_fields = get_enrich_fields(mode)
    batch_name    = f"physician_enrichment_{mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    datas = []
    for row in physicians:
        npi        = row[0]
        first_name = row[1] or ""
        last_name  = row[2] or ""
        org_name   = row[3] or ""
        domain     = row[4] or ""

        contact: dict = {
            "firstname":     first_name,
            "lastname":      last_name,
            "enrich_fields": enrich_fields,
            # Store NPI in custom field so we can match results back
            "custom": {"npi": str(npi)},
        }

        # Prefer domain over company_name — higher match rate
        if domain:
            contact["domain"] = domain
        elif org_name:
            contact["company_name"] = org_name

        datas.append(contact)

    payload = {
        "name":  batch_name,
        "datas": datas,
        # No webhook_url — we poll instead
    }

    print(f"\nSubmitting batch of {len(datas)} physicians to FullEnrich...")
    print(f"  Mode          : {mode}")
    print(f"  Fields        : {enrich_fields}")
    print(f"  Batch name    : {batch_name}")

    try:
        response = requests.post(
            f"{FULLENRICH_BASE}/contact/enrich/bulk",
            headers={
                "Authorization": f"Bearer {FULLENRICH_API_KEY}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=30,
        )

        if response.status_code == 200:
            data           = response.json()
            enrichment_id  = data.get("enrichment_id")
            print(f"  Enrichment ID : {enrichment_id}")
            return enrichment_id

        else:
            print(f"  ERROR: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return None

    except requests.exceptions.Timeout:
        print("  ERROR: Request timed out")
        return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


# ---------------------------------------------------------------------------
# Step 3 — Poll for results
# ---------------------------------------------------------------------------

def poll_for_results(enrichment_id: str) -> Optional[list]:
    """
    Polls the FullEnrich GET endpoint until enrichment is complete.

    FullEnrich processes enrichments asynchronously. This function
    polls every POLL_INTERVAL_SECONDS until status = 'done' or
    until POLL_MAX_ATTEMPTS is reached (5 minutes default).

    Args:
        enrichment_id: UUID returned from the POST submission.

    Returns:
        List of enriched contact dicts, or None if polling failed/timed out.
    """
    print(f"\nPolling for results (enrichment_id: {enrichment_id})")
    print(f"  Checking every {POLL_INTERVAL_SECONDS}s (max {POLL_MAX_ATTEMPTS} attempts)...")

    for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
        time.sleep(POLL_INTERVAL_SECONDS)

        try:
            response = requests.get(
                f"{FULLENRICH_BASE}/contact/enrich/bulk/{enrichment_id}",
                headers={"Authorization": f"Bearer {FULLENRICH_API_KEY}"},
                timeout=15,
            )

            if response.status_code != 200:
                print(f"  Attempt {attempt}: HTTP {response.status_code}")
                continue

            data   = response.json()
            status = data.get("status", "unknown")
            print(f"  Attempt {attempt}: status = {status}")

            # FullEnrich uses 'FINISHED' (uppercase) not 'done'
            if status in ("done", "FINISHED", "finished"):
                contacts = data.get("datas", [])
                print(f"  Done. {len(contacts)} contacts returned.")
                return contacts

            if status in ("failed", "FAILED", "error", "ERROR"):
                print("  ERROR: Enrichment failed on FullEnrich side.")
                return None

            # status = 'pending' or 'processing' — keep polling

        except Exception as e:
            print(f"  Attempt {attempt}: ERROR — {e}")

    print(f"  Timed out after {POLL_MAX_ATTEMPTS} attempts.")
    return None


# ---------------------------------------------------------------------------
# Step 4 — Parse and save results
# ---------------------------------------------------------------------------

def parse_and_save_results(contacts: list, mode: str, now: datetime) -> dict:
    """
    Parses FullEnrich results and saves contact data to the database.

    For each contact with data:
        - Saves personal_email and/or work_email to personal_email column
          (personal email preferred; falls back to work email)
        - Saves mobile_phone if mode = 'full' and phone is returned
        - Appends 'fullenrich' to enrichment_sources[]
        - Sets email_enrichment_attempted = TRUE
        - Updates enrichment_source_stats atomically

    NPI matching:
        Primary — reads from contact.custom.npi (we set this in submission)
        Fallback — match by firstname + lastname from DB

    Args:
        contacts: List of enriched contact dicts from FullEnrich API.
        mode:     'full' or 'email' — determines whether to save phone.
        now:      UTC timestamp for all audit fields.

    Returns:
        Dict with counts: emails_saved, phones_saved, not_found, errors.
    """
    emails_saved = 0
    phones_saved = 0
    not_found    = 0
    errors       = 0

    with engine.connect() as conn:
        for i, contact in enumerate(contacts, 1):

            # ── Resolve NPI ────────────────────────────────────────────
            custom = contact.get("custom") or {}
            npi    = custom.get("npi", "").strip() or None

            if not npi:
                # Fallback — match by name
                first = (contact.get("firstname") or "").strip()
                last  = (contact.get("lastname")  or "").strip()
                if first and last:
                    match = conn.execute(text("""
                        SELECT npi FROM physician
                        WHERE LOWER(first_name_clean) = LOWER(:first)
                          AND LOWER(last_name_clean)  = LOWER(:last)
                        LIMIT 1
                    """), {"first": first, "last": last})
                    row = match.fetchone()
                    if row:
                        npi = row[0]

            if not npi:
                print(f"  [{i}] Cannot match contact to physician — skipping")
                errors += 1
                continue

            # ── Extract contact data ────────────────────────────────────
            # FullEnrich returns separate arrays per field type.
            # Response keys match the enrich_fields we requested:
            #   contact.work_emails     -> contact_data["work_emails"]    list of {email, ...}
            #   contact.personal_emails -> contact_data["personal_emails"] list of {email, ...}
            #   contact.phones          -> contact_data["phones"]          list of {number, ...}
            contact_data    = contact.get("contact") or {}
            work_emails     = contact_data.get("work_emails")     or []
            personal_emails = contact_data.get("personal_emails") or []
            phones          = contact_data.get("phones")          or []

            # Extract best values — take first item from each list
            work_email     = (work_emails[0].get("email")     or "").strip() if work_emails     else ""
            personal_email = (personal_emails[0].get("email") or "").strip() if personal_emails else ""
            phone_number   = (phones[0].get("number")         or "").strip() if phones          else ""

            best_email = personal_email or work_email
            phone      = phone_number

            has_email = bool(best_email)
            has_phone = bool(phone) and mode == "full"

            if not has_email and not has_phone:
                print(f"  [{i}] NPI {npi} — no data returned")
                not_found += 1
                # Mark as attempted by appending to enrichment_sources[].
                # This is the per-source deduplication guard — does NOT set
                # email_enrichment_attempted so other sources can still try.
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
                continue

            print(f"  [{i}] NPI {npi} — email: {best_email} | phone: {phone or 'none'}")

            try:
                # ── physician table ────────────────────────────────────
                conn.execute(text("""
                    UPDATE physician SET
                        personal_email            = COALESCE(:email, personal_email),
                        personal_email_confidence = CASE
                            WHEN :email IS NOT NULL THEN 'HIGH'
                            ELSE personal_email_confidence
                        END,
                        email_enriched_at         = CASE
                            WHEN :email IS NOT NULL THEN :now
                            ELSE email_enriched_at
                        END,
                        mobile_phone              = COALESCE(:phone, mobile_phone),
                        phone_confidence          = CASE
                            WHEN :phone IS NOT NULL THEN 'HIGH'
                            ELSE phone_confidence
                        END,
                        phone_enriched_at         = CASE
                            WHEN :phone IS NOT NULL THEN :now
                            ELSE phone_enriched_at
                        END,
                        email_enrichment_attempted = TRUE,
                        email_enrichment_result   = 'fullenrich_found',
                        enrichment_sources = CASE
                            WHEN enrichment_sources @> ARRAY[:source]::text[]
                            THEN enrichment_sources
                            ELSE array_append(enrichment_sources, :source)
                        END,
                        enrichment_last_attempted_at = :now,
                        updated_at                = :now
                    WHERE npi = :npi
                """), {
                    "npi":    npi,
                    "email":  best_email or None,
                    "phone":  phone or None,
                    "source": SOURCE_NAME,
                    "now":    now,
                })

                # ── audit trail ───────────────────────────────────────
                if best_email:
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

                if phone:
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

                # ── source stats ──────────────────────────────────────
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
                    emails_saved += 1
                if has_phone:
                    phones_saved += 1

            except Exception as e:
                print(f"  [{i}] ERROR saving NPI {npi}: {e}")
                errors += 1
                conn.rollback()

    return {
        "emails_saved": emails_saved,
        "phones_saved": phones_saved,
        "not_found":    not_found,
        "errors":       errors,
    }


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
        count_row = result.fetchone()
        count  = int(count_row[0]) if count_row and count_row[0] is not None else 0
        print(f"  Total leads: {count}")

        cats = conn.execute(text("""
            SELECT contact_category, COUNT(*) AS cnt
            FROM leads GROUP BY contact_category ORDER BY contact_category
        """))
        for r in cats.fetchall():
            print(f"  Category {r[0]}: {r[1]}")


# ---------------------------------------------------------------------------
# Preview mode
# ---------------------------------------------------------------------------

def run_preview(mode: str, limit: Optional[int]) -> None:
    """
    Shows which physicians would be processed without spending any credits.

    Args:
        mode:  'full' or 'email'.
        limit: Max records to show.
    """
    physicians = get_physicians_to_enrich(mode=mode, limit=limit)
    if not physicians:
        print("No eligible physicians found.")
        return

    print(f"\n{'NPI':<12} {'Name':<30} {'Org/Domain':<40} Score")
    print("-" * 90)
    for row in physicians:
        name    = f"{row[1] or ''} {row[2] or ''}".strip()
        context = row[4] or row[3] or "—"
        print(f"{row[0]:<12} {name:<30} {context:<40} {row[5]}")

    credits_per = 14 if mode == "full" else 4
    print(f"\nEstimated credit cost: {len(physicians)} × {credits_per} = "
          f"{len(physicians) * credits_per} credits")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_enrichment(
    mode:  str,
    limit: Optional[int] = None,
    npi:   Optional[str] = None,
) -> None:
    """
    Orchestrates the full FullEnrich enrichment flow.

    Args:
        mode:  'full' (phone+email) or 'email' (email only).
        limit: Max physicians to process.
        npi:   Single NPI for test mode.
    """
    now = datetime.now(timezone.utc)

    print("=" * 60)
    print(f"FULLENRICH ENRICHMENT — mode: {mode.upper()}")
    print(f"Started: {now.isoformat()}")
    print("=" * 60)

    if not FULLENRICH_API_KEY:
        print("ERROR: FULLENRICH_API_KEY not set in .env")
        return

    physicians = get_physicians_to_enrich(mode=mode, limit=limit, npi=npi)
    if not physicians:
        print("No physicians to enrich.")
        return

    enrichment_id = submit_enrichment_batch(physicians, mode)
    if not enrichment_id:
        print("Batch submission failed — aborting.")
        return

    contacts = poll_for_results(enrichment_id)
    if contacts is None:
        print("Could not retrieve results — check FullEnrich dashboard manually.")
        print(f"Enrichment ID: {enrichment_id}")
        return

    counts = parse_and_save_results(contacts, mode, now)
    _sync_leads(now)

    total = len(physicians)
    print()
    print("=" * 60)
    print("FULLENRICH ENRICHMENT COMPLETE")
    print(f"  Total submitted : {total}")
    print(f"  Emails saved    : {counts['emails_saved']}")
    print(f"  Phones saved    : {counts['phones_saved']}")
    print(f"  No data found   : {counts['not_found']}")
    print(f"  Errors          : {counts['errors']}")
    if total > 0:
        print(f"  Email hit rate  : {counts['emails_saved'] / total * 100:.1f}%")
        if mode == "full":
            print(f"  Phone hit rate  : {counts['phones_saved'] / total * 100:.1f}%")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FullEnrich Physician Enrichment Pipeline"
    )
    parser.add_argument(
        "--mode", choices=["full", "email"], default="email",
        help="full = phone+email (14 credits each). email = email only (4 credits each)."
    )
    parser.add_argument(
        "--limit", type=int,
        help="Max physicians to process"
    )
    parser.add_argument(
        "--npi", type=str,
        help="Process a single physician by NPI (test mode)"
    )
    parser.add_argument(
        "--preview", action="store_true",
        help="Show who would be processed without spending credits"
    )
    parser.add_argument(
        "--fetch", type=str, metavar="ENRICHMENT_ID",
        help="Fetch and save results for an already-completed enrichment by ID"
    )
    args = parser.parse_args()

    if args.preview:
        run_preview(mode=args.mode, limit=args.limit)
    elif args.fetch:
        # Fetch results for a completed enrichment without re-submitting
        now      = datetime.now(timezone.utc)
        contacts = poll_for_results(args.fetch)
        if contacts:
            counts = parse_and_save_results(contacts, args.mode, now)
            _sync_leads(now)
            print(f"Fetched: emails={counts['emails_saved']} phones={counts['phones_saved']}")
    else:
        run_enrichment(mode=args.mode, limit=args.limit, npi=args.npi)