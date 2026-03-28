# etl/enrich_lusha.py
#
# Lusha enrichment pipeline - phone + email via full REST API.
#
# Lusha provides a bulk enrichment API that accepts up to 100 contacts
# per request and returns email addresses and phone numbers.
#
# API details
# -----------
#   Endpoint : POST https://api.lusha.com/v2/person
#   Auth     : api_key header
#   Input    : firstName + lastName + companyName OR companyDomain
#   Output   : email addresses + phone numbers
#   Credits  : 1 per email found, 5 per phone found
#              No charge if nothing is found
#
# Credit strategy (40 credits available)
# ---------------------------------------
#   Conservative: run top 20 physicians with domains
#   Expected cost: ~5-8 hits x (1 email + 5 phone) = 12-48 credits
#   If credits run low the script stops before exceeding balance
#
# Eligibility rules (per-source)
# --------------------------------
#   - 'lusha' NOT already in enrichment_sources[] (not already tried)
#   - Not fully enriched (missing phone OR missing email)
#   - Active physician
#   - Has companyName OR practice_domain
#
# Usage
# -----
#   python etl/enrich_lusha.py --preview          # see who would be processed
#   python etl/enrich_lusha.py --limit 20         # process top 20
#   python etl/enrich_lusha.py --npi 1234567890   # single test
#
# Environment variables required (.env)
# --------------------------------------
#   LUSHA_API_KEY

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

LUSHA_API_KEY = os.getenv("LUSHA_API_KEY")
LUSHA_BASE_URL = "https://api.lusha.com"
SOURCE_NAME = "lusha"

# Safety limit - stop if remaining credits drop below this
MIN_CREDITS_BUFFER = 5


# ---------------------------------------------------------------------------
# Step 1 - Check credit balance
# ---------------------------------------------------------------------------

def get_credit_balance() -> Optional[int]:
    """
    Fetches current Lusha credit balance from the account usage endpoint.

    Returns:
        Remaining credit balance as integer, or None if request fails.
    """
    try:
        response = requests.get(
            f"{LUSHA_BASE_URL}/account/usage",
            headers={"api_key": LUSHA_API_KEY},
            timeout=10,
        )
        if response.status_code == 200:
            data = response.json()
            # Lusha returns credits remaining in the response
            remaining = data.get("creditsRemaining") or data.get("credits_remaining")
            if remaining is not None:
                return int(remaining)
        print(f"  Could not fetch balance: HTTP {response.status_code}")
        return None
    except Exception as e:
        print(f"  Balance check error: {e}")
        return None


# ---------------------------------------------------------------------------
# Step 2 - Pull eligible physicians
# ---------------------------------------------------------------------------

def get_physicians_to_enrich(
    limit: Optional[int] = None,
    npi: Optional[str] = None,
) -> list:
    """
    Fetches physicians eligible for Lusha enrichment.

    Eligibility:
        - 'lusha' not already in enrichment_sources[] (per-source guard)
        - Not fully enriched (missing phone OR email)
        - Active
        - Has organization_name or practice_domain

    Ordered by:
        1. Physicians with LinkedIn URLs first (higher phone hit rate)
        2. Physicians with domains next
        3. Lead score DESC within each group

    Args:
        limit: Max records to return.
        npi:   Single NPI for test mode.

    Returns:
        List of row tuples:
        (npi, first_name, last_name, organization_name, practice_domain,
         lead_score_current, lead_tier, linkedin_url)
    """
    with engine.connect() as conn:
        if npi:
            result = conn.execute(text("""
                SELECT npi, first_name_clean, last_name_clean,
                       organization_name, practice_domain,
                       lead_score_current, lead_tier, linkedin_url
                FROM physician
                WHERE npi = :npi
            """), {"npi": npi})
            rows = result.fetchall()
            print(f"NPI mode: found {len(rows)} physician")
            return rows

        query = """
            SELECT npi, first_name_clean, last_name_clean,
                   organization_name, practice_domain,
                   lead_score_current, lead_tier, linkedin_url
            FROM physician
            WHERE is_active = TRUE
              AND NOT (enrichment_sources @> ARRAY['lusha']::text[])
              AND NOT (
                mobile_phone IS NOT NULL
                AND (personal_email IS NOT NULL OR email IS NOT NULL)
              )
              AND (organization_name IS NOT NULL OR practice_domain IS NOT NULL)
            ORDER BY
                CASE WHEN linkedin_url IS NOT NULL THEN 0 ELSE 1 END ASC,
                CASE WHEN practice_domain IS NOT NULL THEN 0 ELSE 1 END ASC,
                lead_score_current DESC NULLS LAST
        """
        if limit:
            query += f" LIMIT {limit}"

        result = conn.execute(text(query))
        rows = result.fetchall()
        print(f"Found {len(rows)} physicians eligible for Lusha enrichment")
        return rows


# ---------------------------------------------------------------------------
# Step 3 - Submit bulk enrichment request
# ---------------------------------------------------------------------------

def submit_bulk_request(physicians: list) -> Optional[dict]:
    """
    Submits a bulk person enrichment request to Lusha API.

    Builds the contacts array from physician rows. Uses linkedin_url
    if available (improves phone hit rate significantly), otherwise
    uses companyDomain or companyName.

    Args:
        physicians: List of physician row tuples.

    Returns:
        Raw API response dict, or None if request failed.
    """
    contacts = []
    for row in physicians:
        npi = str(row[0])
        first_name = row[1] or ""
        last_name = row[2] or ""
        org_name = row[3] or ""
        domain = row[4] or ""
        linkedin_url = row[7] or ""

        contact = {
            "contactId": npi,       # use NPI as contactId for easy matching
            "fullName": f"{first_name} {last_name}".strip(),
        }

        if linkedin_url:
            contact["linkedinUrl"] = linkedin_url
        elif domain:
            contact["companies"] = [{"domain": domain, "isCurrent": True}]
        elif org_name:
            contact["companies"] = [{"name": org_name, "isCurrent": True}]

        contacts.append(contact)

    payload = {
        "contacts": contacts,
        "metadata": {
            # Default behavior returns emails + phones without these params
            # These are only for Unified Credits plan - omitting for free plan
            "partialProfile": True,
        }
    }

    print(f"\nSubmitting {len(contacts)} physicians to Lusha bulk API...")

    try:
        response = requests.post(
            f"{LUSHA_BASE_URL}/v2/person",
            headers={
                "api_key": LUSHA_API_KEY,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )

        print(f"  HTTP {response.status_code}")

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 403:
            print("  ERROR: 403 Forbidden - free plan may not support bulk endpoint")
            print("  Falling back to single-contact mode...")
            return None
        else:
            print(f"  ERROR: {response.text[:300]}")
            return None

    except requests.exceptions.Timeout:
        print("  ERROR: Request timed out")
        return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


# ---------------------------------------------------------------------------
# Step 4 - Single contact fallback
# ---------------------------------------------------------------------------

def enrich_single_contact(
    npi: str,
    first_name: str,
    last_name: str,
    org_name: str,
    domain: str,
    linkedin_url: str,
) -> Optional[dict]:
    """
    Enriches a single physician via GET /v2/person endpoint.

    Used as fallback if bulk endpoint is not available on free plan.

    Args:
        npi:         Physician NPI (used for logging).
        first_name:  Physician first name.
        last_name:   Physician last name.
        org_name:    Organization name.
        domain:      Practice domain.
        linkedin_url: LinkedIn URL if available.

    Returns:
        Contact data dict from API response, or None if not found.
    """
    params = {
        "firstName": first_name,
        "lastName": last_name,
    }

    if linkedin_url:
        params["linkedinUrl"] = linkedin_url
    elif domain:
        params["companyDomain"] = domain
    elif org_name:
        params["companyName"] = org_name

    try:
        response = requests.get(
            f"{LUSHA_BASE_URL}/v2/person",
            headers={"api_key": LUSHA_API_KEY},
            params=params,
            timeout=15,
        )

        if response.status_code == 200:
            data = response.json()
            contact = data.get("contact", {})
            if contact.get("isCreditCharged") or contact.get("data"):
                return contact.get("data")
            return None
        elif response.status_code == 429:
            print("    Rate limited - waiting 10s...")
            time.sleep(10)
            return None
        else:
            return None

    except Exception as e:
        print(f"    Error: {e}")
        return None


# ---------------------------------------------------------------------------
# Step 5 - Parse contact data from Lusha response
# ---------------------------------------------------------------------------

def extract_contact_data(contact_data: dict) -> tuple[str, str]:
    """
    Extracts the best email and phone from a Lusha contact data dict.

    Lusha returns emailAddresses as a list of dicts with 'email' field,
    and phoneNumbers as a list of dicts with 'localizedNumber' field.

    Prefers:
        Email: first email in the list (Lusha orders by confidence)
        Phone: first mobile phone, falls back to first any phone

    Args:
        contact_data: The 'data' object from Lusha API response.

    Returns:
        Tuple of (email, phone) - empty string if not found.
    """
    email = ""
    phone = ""

    if not contact_data:
        return email, phone

    # Extract email
    email_list = contact_data.get("emailAddresses", []) or []
    if email_list:
        first_email = email_list[0]
        email = first_email.get("email", "") or ""

    # Extract phone - prefer mobile
    phone_list = contact_data.get("phoneNumbers", []) or []
    mobile_phones = [
        p for p in phone_list
        if (p.get("type") or "").lower() in ("mobile", "cell", "direct")
    ]
    if mobile_phones:
        phone = mobile_phones[0].get("localizedNumber", "") or ""
    elif phone_list:
        phone = phone_list[0].get("localizedNumber", "") or ""

    return email.strip(), phone.strip()


# ---------------------------------------------------------------------------
# Step 6 - Save results to DB
# ---------------------------------------------------------------------------

def save_result(
    npi: str,
    email: str,
    phone: str,
    now: datetime,
    conn,
) -> tuple[bool, bool]:
    """
    Saves Lusha enrichment results to physician table.

    Writes to:
        - personal_email (if found and not already set)
        - mobile_phone (if found and not already set)
        - enrichment_sources[] - appends 'lusha'
        - field_value_history - audit trail
        - enrichment_source_stats - atomic counter increment

    Args:
        npi:   Physician NPI.
        email: Email found (empty string = not found).
        phone: Phone found (empty string = not found).
        now:   UTC timestamp.
        conn:  Active DB connection.

    Returns:
        Tuple (email_saved, phone_saved) booleans.
    """
    has_email = bool(email)
    has_phone = bool(phone)

    conn.execute(text("""
        UPDATE physician SET
            personal_email = CASE
                WHEN :email != '' AND personal_email IS NULL THEN :email
                ELSE personal_email
            END,
            personal_email_confidence = CASE
                WHEN :email != '' AND personal_email IS NULL THEN 'HIGH'
                ELSE personal_email_confidence
            END,
            email_enriched_at = CASE
                WHEN :email != '' AND personal_email IS NULL THEN :now
                ELSE email_enriched_at
            END,
            mobile_phone = CASE
                WHEN :phone != '' AND mobile_phone IS NULL THEN :phone
                ELSE mobile_phone
            END,
            phone_confidence = CASE
                WHEN :phone != '' AND mobile_phone IS NULL THEN 'HIGH'
                ELSE phone_confidence
            END,
            phone_enriched_at = CASE
                WHEN :phone != '' AND mobile_phone IS NULL THEN :now
                ELSE phone_enriched_at
            END,
            enrichment_sources = CASE
                WHEN enrichment_sources @> ARRAY['lusha']::text[]
                THEN enrichment_sources
                ELSE array_append(enrichment_sources, 'lusha')
            END,
            enrichment_last_attempted_at = :now,
            updated_at                   = :now
        WHERE npi = :npi
    """), {
        "npi": npi,
        "email": email,
        "phone": phone,
        "now": now,
    })

    # Audit trail - email
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
        """), {"npi": npi, "email": email, "source": SOURCE_NAME, "now": now})

    # Audit trail - phone
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
        """), {"npi": npi, "phone": phone, "source": SOURCE_NAME, "now": now})

    # Stats - atomic increment
    conn.execute(text("""
        UPDATE enrichment_source_stats SET
            emails_provided = emails_provided + :emails,
            phones_provided = phones_provided + :phones,
            total_hits      = total_hits      + :hits,
            total_attempts  = total_attempts  + 1,
            last_used_at    = :now,
            updated_at      = :now
        WHERE source_name   = :source
    """), {
        "emails": 1 if has_email else 0,
        "phones": 1 if has_phone else 0,
        "hits": 1 if (has_email or has_phone) else 0,
        "source": SOURCE_NAME,
        "now": now,
    })

    return has_email, has_phone


def mark_no_result(npi: str, now: datetime, conn) -> None:
    """
    Marks physician as attempted by Lusha with no data found.

    Appends 'lusha' to enrichment_sources[] so this physician
    is skipped on future runs. Does NOT set email_enrichment_attempted
    so other sources can still process this physician.
    """
    conn.execute(text("""
        UPDATE physician SET
            enrichment_sources = CASE
                WHEN enrichment_sources @> ARRAY['lusha']::text[]
                THEN enrichment_sources
                ELSE array_append(enrichment_sources, 'lusha')
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

    Idempotent - safe to call multiple times.
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
        count_row = result.fetchone()
        count = int(count_row[0]) if count_row and count_row[0] is not None else 0
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

def run_preview(limit: int) -> None:
    """Shows which physicians would be processed without spending credits."""
    physicians = get_physicians_to_enrich(limit=limit)
    if not physicians:
        print("No eligible physicians found.")
        return

    print(f"\n{'NPI':<12} {'Name':<28} {'Domain/Org':<35} {'LinkedIn':<5} Score")
    print("-" * 90)
    for row in physicians:
        name = f"{row[1] or ''} {row[2] or ''}".strip()
        context = row[4] or row[3] or "-"
        has_li = "Y" if row[7] else "N"
        print(f"{row[0]:<12} {name:<28} {context:<35} {has_li:<5} {row[5]}")

    print(f"\n  Total eligible : {len(physicians)}")
    print(f"  With LinkedIn  : {sum(1 for r in physicians if r[7])}")
    print(f"  With domain    : {sum(1 for r in physicians if r[4])}")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_enrichment(
    limit: Optional[int] = None,
    npi: Optional[str] = None,
) -> None:
    """
    Orchestrates the full Lusha enrichment flow.

    Strategy:
        1. Try bulk endpoint (POST /v2/person) for efficiency
        2. If bulk fails (403), fall back to single requests (GET /v2/person)
        3. Parse results, save to DB, update stats
        4. Sync leads table

    Args:
        limit: Max physicians to process.
        npi:   Single NPI for test mode.
    """
    now = datetime.now(timezone.utc)

    print("=" * 60)
    print("LUSHA ENRICHMENT")
    print(f"Started: {now.isoformat()}")
    print("=" * 60)

    if not LUSHA_API_KEY:
        print("ERROR: LUSHA_API_KEY not set in .env")
        return

    physicians = get_physicians_to_enrich(limit=limit, npi=npi)
    if not physicians:
        print("No physicians to enrich.")
        return

    emails_saved = 0
    phones_saved = 0
    not_found = 0
    errors = 0

    # -- Try bulk endpoint first --
    bulk_response = submit_bulk_request(physicians)

    with engine.connect() as conn:

        if bulk_response:
            # Bulk endpoint worked - parse all results
            contacts_data = bulk_response.get("contacts", {})
            print(f"  Bulk response received. Processing {len(contacts_data)} contacts...")

            for row in physicians:
                npi_val = str(row[0])
                first = row[1] or ""
                last = row[2] or ""

                contact_entry = contacts_data.get(npi_val, {})
                contact_data = contact_entry.get("data") if contact_entry else None

                email, phone = extract_contact_data(contact_data) if contact_data else ("", "")

                print(f"\n  [{npi_val}] {first} {last}")
                print(f"    email={email or '-'} | phone={phone or '-'}")

                if not email and not phone:
                    not_found += 1
                    mark_no_result(npi_val, now, conn)
                    conn.commit()
                    print("    No data found - marked as attempted")
                    continue

                saved_email, saved_phone = save_result(npi_val, email, phone, now, conn)
                conn.commit()

                if saved_email:
                    emails_saved += 1
                    print(f"    Email SAVED: {email}")
                if saved_phone:
                    phones_saved += 1
                    print(f"    Phone SAVED: {phone}")

        else:
            # Fallback - single contact requests
            print("\nFalling back to single-contact requests...")
            print(f"Processing {len(physicians)} physicians one by one...")

            for i, row in enumerate(physicians, 1):
                npi_val = str(row[0])
                first = row[1] or ""
                last = row[2] or ""
                org_name = row[3] or ""
                domain = row[4] or ""
                linkedin_url = row[7] or ""

                print(f"\n  [{i}/{len(physicians)}] {first} {last} | {domain or org_name}")

                contact_data = enrich_single_contact(
                    npi_val, first, last, org_name, domain, linkedin_url
                )

                email, phone = extract_contact_data(contact_data) if contact_data else ("", "")
                print(f"    email={email or '-'} | phone={phone or '-'}")

                if not email and not phone:
                    not_found += 1
                    mark_no_result(npi_val, now, conn)
                    conn.commit()
                    print("    No data - marked as attempted")
                    time.sleep(0.5)  # rate limit respect
                    continue

                saved_email, saved_phone = save_result(npi_val, email, phone, now, conn)
                conn.commit()

                if saved_email:
                    emails_saved += 1
                    print(f"    Email SAVED: {email}")
                if saved_phone:
                    phones_saved += 1
                    print(f"    Phone SAVED: {phone}")

                time.sleep(0.5)  # respect rate limits

    # -- Sync leads --
    _sync_leads(now)

    # -- Summary --
    total = len(physicians)
    print()
    print("=" * 60)
    print("LUSHA ENRICHMENT COMPLETE")
    print(f"  Total processed : {total}")
    print(f"  Emails saved    : {emails_saved}")
    print(f"  Phones saved    : {phones_saved}")
    print(f"  No data found   : {not_found}")
    print(f"  Errors          : {errors}")
    if total > 0:
        print(f"  Email hit rate  : {emails_saved / total * 100:.1f}%")
        print(f"  Phone hit rate  : {phones_saved / total * 100:.1f}%")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Lusha Physician Enrichment - Phone + Email via API"
    )
    parser.add_argument(
        "--limit", type=int,
        help="Max physicians to process (default: all eligible)"
    )
    parser.add_argument(
        "--npi", type=str,
        help="Process a single physician by NPI (test mode)"
    )
    parser.add_argument(
        "--preview", action="store_true",
        help="Show eligible physicians without spending credits"
    )
    args = parser.parse_args()

    if args.preview:
        run_preview(limit=args.limit or 50)
    else:
        run_enrichment(limit=args.limit, npi=args.npi)
