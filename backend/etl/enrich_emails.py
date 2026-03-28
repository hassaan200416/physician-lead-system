# etl/enrich_emails.py
#
# Email enrichment pipeline — Hunter.io + free pre-filters.
#
# This script is the reference implementation for ALL enrichment sources.
# Every other source (Apollo, ContactOut, etc.) follows the same pattern:
#   1. Pull eligible physicians (not yet attempted by this source)
#   2. Call the source API or process imported CSV
#   3. Write results to personal_email (canonical) + legacy email (compat)
#   4. Append source name to physician.enrichment_sources[]
#   5. Update enrichment_source_stats counters atomically
#   6. Sync leads table with new contact_category
#
# Hunter.io two-pass strategy
# ---------------------------
#   Pass 1: company name → Hunter resolves domain + returns email + score
#   Pass 2: if score < 70, retry with the discovered domain directly
#           (bypasses Hunter's domain guessing, typically +15-25 pts)
#
# Free pre-filters (5 layers, run after Hunter returns an email)
# --------------------------------------------------------------
#   1. Syntax check       — valid email format
#   2. Disposable check   — not a throwaway provider
#   3. Domain DNS check   — domain exists on the internet
#   4. MX record check    — domain can receive email
#   5. Catch-all check    — mail server does not accept all addresses
#
# Confidence levels
# -----------------
#   HIGH   — Hunter verified OR score >= 70 AND not catch-all
#   MEDIUM — score 40-69 AND not catch-all
#   LOW    — catch-all domain (email exists but unverifiable)
#
# Usage
# -----
#   python etl/enrich_emails.py --limit 25      # process up to 25 physicians
#   python etl/enrich_emails.py --npi 1234567890 # single physician (test mode)
#
# Environment variables required (.env)
# --------------------------------------
#   HUNTER_API_KEY

import sys
import time
import argparse
import re
import dns.resolver
import smtplib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, TypedDict, NotRequired, cast

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import engine
from dotenv import load_dotenv
import os

load_dotenv()

HUNTER_API_KEY             = os.getenv("HUNTER_API_KEY")
HUNTER_EMAIL_FINDER_URL    = "https://api.hunter.io/v2/email-finder"

# Source name must match exactly what is seeded in enrichment_source_stats
SOURCE_NAME                = "hunter.io"

MIN_SCORE_TO_PROCEED       = 40   # discard anything below this
MIN_SCORE_HIGH_CONFIDENCE  = 70   # trigger Pass 2 if below this
HUNTER_RATE_LIMIT_DELAY    = 1.5  # seconds between API calls


# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------

class PreFilterResult(TypedDict):
    passed:      bool
    reason:      str
    is_catch_all: NotRequired[bool]
    domain:      NotRequired[str]


class HunterResult(TypedDict):
    success:             bool
    email:               NotRequired[Optional[str]]
    score:               NotRequired[int]
    domain:              NotRequired[Optional[str]]
    verification_status: NotRequired[Optional[str]]
    error:               NotRequired[str]


# ---------------------------------------------------------------------------
# Step 1 — Pull physicians eligible for enrichment
# ---------------------------------------------------------------------------

def get_physicians_to_enrich(
    limit: Optional[int] = None,
    npi:   Optional[str] = None,
):
    """
    Fetches physicians eligible for Hunter.io email enrichment.

    Eligibility (batch mode):
        - personal_email IS NULL          — no canonical email yet
        - email IS NULL                   — no legacy email either
        - organization_name IS NOT NULL   — required for Hunter lookup
        - is_active = TRUE
        - email_enrichment_attempted = FALSE — not yet processed

    Results are ordered by lead_score_current DESC so the highest-value
    leads consume the limited Hunter free-tier credits first.

    NPI mode bypasses all filters — used for single-record testing.

    Args:
        limit: Max physicians to return (use 25 for Hunter free tier).
        npi:   Single NPI to process regardless of enrichment status.

    Returns:
        List of row tuples:
        (npi, first_name_clean, last_name_clean, organization_name,
         practice_domain, lead_score_current, lead_tier)
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
        else:
            query = """
                SELECT npi, first_name_clean, last_name_clean,
                       organization_name, practice_domain,
                       lead_score_current, lead_tier
                FROM physician
                WHERE personal_email IS NULL
                  AND email IS NULL
                  AND organization_name IS NOT NULL
                  AND is_active = TRUE
                  AND NOT (enrichment_sources @> ARRAY['hunter.io']::text[])
                ORDER BY lead_score_current DESC NULLS LAST
            """
            if limit:
                query += f" LIMIT {limit}"
            result = conn.execute(text(query))

        rows = result.fetchall()
        print(f"Found {len(rows)} physicians eligible for enrichment")
        return rows


# ---------------------------------------------------------------------------
# Step 2 — Free pre-filters
# ---------------------------------------------------------------------------

def check_syntax(email: str) -> bool:
    """
    Validates basic email format with regex.

    Pattern: localpart@domain.tld (TLD >= 2 chars).

    Returns:
        True if format is valid.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def check_domain_exists(domain: str) -> bool:
    """
    Checks the domain has a DNS A record.

    A domain without an A record is fake, expired, or misconfigured.
    Emails will always bounce.

    Returns:
        True if DNS A record found.
    """
    try:
        dns.resolver.resolve(domain, 'A')
        return True
    except Exception:
        return False


def check_mx_record(domain: str) -> bool:
    """
    Checks the domain has MX (Mail Exchange) records.

    A domain without MX records cannot receive email even if it exists.

    Returns:
        True if at least one MX record found.
    """
    try:
        dns.resolver.resolve(domain, 'MX')
        return True
    except Exception:
        return False


def check_catch_all(domain: str) -> bool:
    """
    Detects if a mail server accepts all incoming addresses (catch-all).

    Sends SMTP RCPT TO with a random address. If accepted (code 250),
    individual address verification is unreliable — every address
    at this domain will appear valid.

    Returns:
        True if catch-all detected (email present but unverifiable).
        False if not catch-all or probe fails (assumed safe to proceed).
    """
    try:
        random_email = f"randomtest_{uuid.uuid4().hex[:8]}@{domain}"
        mx_records   = dns.resolver.resolve(domain, 'MX')
        first_record = next(iter(mx_records), None)
        if first_record is None:
            return False
        mx_host = str(first_record).split()[-1].rstrip('.')
        with smtplib.SMTP(timeout=5) as smtp:
            smtp.connect(mx_host)
            smtp.ehlo()
            smtp.mail('')
            code, _ = smtp.rcpt(random_email)
            return code == 250
    except Exception:
        return False


DISPOSABLE_DOMAINS = {
    'mailinator.com', 'tempmail.com', 'throwaway.email',
    'guerrillamail.com', 'yopmail.com', 'trashmail.com',
    'sharklasers.com', 'guerrillamailblock.com', 'grr.la',
    'spam4.me', 'dispostable.com', 'mailnull.com',
}


def check_disposable(domain: str) -> bool:
    return domain.lower() in DISPOSABLE_DOMAINS


def run_free_prefilters(email: str) -> PreFilterResult:
    """
    Runs all 5 free verification layers on an email address.

    Stops at the first failure to avoid unnecessary network calls.

    Layers (in order):
        1. Syntax check
        2. Disposable domain check
        3. Domain DNS A record
        4. MX record
        5. Catch-all probe

    Args:
        email: Full email address to validate.

    Returns:
        PreFilterResult — passed=True only if all 5 layers clear.
    """
    domain = email.split('@')[1] if '@' in email else ''

    if not check_syntax(email):
        return {"passed": False, "reason": "invalid_syntax"}
    if check_disposable(domain):
        return {"passed": False, "reason": "disposable_domain"}
    if not check_domain_exists(domain):
        return {"passed": False, "reason": "domain_not_found"}
    if not check_mx_record(domain):
        return {"passed": False, "reason": "no_mx_record"}

    is_catch_all = check_catch_all(domain)
    return {
        "passed":      True,
        "reason":      "all_checks_passed",
        "is_catch_all": is_catch_all,
        "domain":      domain,
    }


# ---------------------------------------------------------------------------
# Step 3 — Hunter.io API calls
# ---------------------------------------------------------------------------

def call_hunter(
    first_name: str,
    last_name:  str,
    company:    Optional[str] = None,
    domain:     Optional[str] = None,
) -> HunterResult:
    """
    Calls Hunter.io Email Finder API.

    Pass 1 — company name: Hunter resolves domain internally.
    Pass 2 — direct domain: bypasses domain guessing, higher accuracy.

    Exactly one of company or domain must be provided.

    Args:
        first_name: Physician's cleaned first name.
        last_name:  Physician's cleaned last name.
        company:    Organisation name (Pass 1).
        domain:     Practice domain e.g. 'sepath.com' (Pass 2).

    Returns:
        HunterResult dict. Check success=True before reading other fields.
    """
    params: dict[str, str] = {
        "first_name": first_name,
        "last_name":  last_name,
        "api_key":    HUNTER_API_KEY or "",
    }
    if domain:
        params["domain"]  = domain
    elif company:
        params["company"] = company

    try:
        response = requests.get(
            HUNTER_EMAIL_FINDER_URL,
            params=params,
            timeout=15,
        )
        if response.status_code == 200:
            data       = cast(dict[str, Any], response.json())
            data_obj   = cast(dict[str, Any], data.get("data", {}))
            verify_obj = cast(dict[str, Any], data_obj.get("verification") or {})
            score_raw  = data_obj.get("score", 0)
            score_val  = int(score_raw) if isinstance(score_raw, (int, float)) else 0
            return {
                "success":             True,
                "email":               cast(Optional[str], data_obj.get("email")),
                "score":               score_val,
                "domain":              cast(Optional[str], data_obj.get("domain")),
                "verification_status": cast(Optional[str], verify_obj.get("status")),
            }
        elif response.status_code == 401:
            return {"success": False, "error": "invalid_api_key"}
        elif response.status_code == 429:
            return {"success": False, "error": "rate_limited"}
        else:
            return {"success": False, "error": f"http_{response.status_code}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "timeout"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Step 4 — Database writes
# ---------------------------------------------------------------------------

def save_email_to_db(
    npi:                 str,
    email:               str,
    score:               int,
    domain:              str,
    verification_status: str,
    confidence_level:    str,
    pass_number:         int,
    now:                 datetime,
) -> None:
    """
    Persists a verified email to the database.

    Writes to three places in a single transaction:
        1. physician table
           - personal_email (canonical new column)
           - personal_email_confidence
           - email_enriched_at
           - email_enrichment_attempted = TRUE
           - enrichment_sources[] — appends 'hunter.io' if not already present
           - enrichment_last_attempted_at
           - Legacy columns (email, email_confidence_*) for backward compat
        2. field_value_history — full audit trail entry
        3. enrichment_source_stats — increments counters atomically

    Args:
        npi:                 10-digit NPI.
        email:               Verified email address.
        score:               Hunter confidence score 0-100.
        domain:              Practice domain extracted from email.
        verification_status: How the email was verified.
        confidence_level:    HIGH | MEDIUM | LOW.
        pass_number:         1 = Pass 1 result, 2 = Pass 2 upgrade.
        now:                 UTC timestamp for all audit fields.
    """
    with engine.connect() as conn:

        # ── physician table ────────────────────────────────────────────────
        conn.execute(text("""
            UPDATE physician SET
                -- canonical new columns
                personal_email            = :email,
                personal_email_confidence = :confidence_level,
                email_enriched_at         = :now,
                -- deduplication guard
                email_enrichment_attempted = TRUE,
                -- append source if not already in array
                enrichment_sources = CASE
                    WHEN enrichment_sources @> ARRAY[:source_name]::text[]
                    THEN enrichment_sources
                    ELSE array_append(enrichment_sources, :source_name)
                END,
                enrichment_last_attempted_at = :now,
                -- legacy columns — kept for backward compat
                email                     = :email,
                email_confidence_score    = :score,
                email_confidence_level    = :confidence_level,
                email_verification_status = :verification_status,
                email_source              = :source_name,
                email_acquired_at         = :now,
                email_enrichment_result   = :result,
                practice_domain           = COALESCE(practice_domain, :domain),
                updated_at                = :now
            WHERE npi = :npi
        """), {
            "npi":                npi,
            "email":              email,
            "score":              score,
            "confidence_level":   confidence_level,
            "verification_status": verification_status,
            "source_name":        SOURCE_NAME,
            "domain":             domain,
            "result":             f"found_pass{pass_number}",
            "now":                now,
        })

        # ── audit trail ───────────────────────────────────────────────────
        conn.execute(text("""
            INSERT INTO field_value_history (
                history_id, entity_type, entity_id, npi, field_name,
                field_value, source_name, confidence_score, is_current,
                collected_timestamp, created_at
            ) VALUES (
                gen_random_uuid(), 'physician', :npi, :npi, 'personal_email',
                :email, :source_name, :conf_score, TRUE, :now, :now
            )
        """), {
            "npi":         npi,
            "email":       email,
            "source_name": SOURCE_NAME,
            "conf_score":  score / 100,
            "now":         now,
        })

        # ── source stats — atomic increment ───────────────────────────────
        conn.execute(text("""
            UPDATE enrichment_source_stats SET
                emails_provided = emails_provided + 1,
                total_hits      = total_hits      + 1,
                total_attempts  = total_attempts  + 1,
                last_used_at    = :now,
                updated_at      = :now
            WHERE source_name = :source_name
        """), {"source_name": SOURCE_NAME, "now": now})

        conn.commit()


def mark_enrichment_failed(npi: str, reason: str, now: datetime) -> None:
    """
    Marks a physician as attempted but no email found.

    Sets email_enrichment_attempted = TRUE so this physician is
    skipped on the next run. Also increments total_attempts in
    enrichment_source_stats so hit rate stays accurate.

    Args:
        npi:    Physician NPI.
        reason: Short failure reason e.g. 'no_result', 'low_score_35'.
        now:    UTC timestamp.
    """
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE physician SET
                -- Append source to array — per-source dedup guard.
                -- Does NOT set email_enrichment_attempted so other
                -- sources can still process this physician.
                enrichment_sources = CASE
                    WHEN enrichment_sources @> ARRAY['hunter.io']::text[]
                    THEN enrichment_sources
                    ELSE array_append(enrichment_sources, 'hunter.io')
                END,
                email_enrichment_result      = :reason,
                enrichment_last_attempted_at = :now,
                updated_at                   = :now
            WHERE npi = :npi
        """), {"npi": npi, "reason": reason, "now": now})

        # Count the attempt even when no email was found
        conn.execute(text("""
            UPDATE enrichment_source_stats SET
                total_attempts = total_attempts + 1,
                last_used_at   = :now,
                updated_at     = :now
            WHERE source_name = :source_name
        """), {"source_name": SOURCE_NAME, "now": now})

        conn.commit()


def store_domain(npi: str, domain: str, now: datetime) -> None:
    """
    Persists the practice domain discovered during Pass 1.

    Stored even when score is too low to save an email. Enables
    future Pass 2 calls without re-running Pass 1.
    Only updates if practice_domain is currently NULL.

    Args:
        npi:    Physician NPI.
        domain: Discovered domain e.g. 'sepath.com'.
        now:    UTC timestamp.
    """
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE physician SET
                practice_domain = :domain,
                updated_at      = :now
            WHERE npi = :npi
              AND practice_domain IS NULL
        """), {"npi": npi, "domain": domain, "now": now})
        conn.commit()


# ---------------------------------------------------------------------------
# Step 5 — Sync to leads table
# ---------------------------------------------------------------------------

def sync_to_leads_table(now: datetime) -> None:
    """
    Syncs physicians with contact data into the leads table.

    Contact category rules (applied here and stored on each row):
        A = mobile_phone present AND any email present
        B = any email present, no mobile_phone
        EXCLUDED = no contact info — never inserted

    Uses INSERT ... ON CONFLICT DO UPDATE (upsert) so it is safe
    to call multiple times — fully idempotent.

    Physicians with no contact info are skipped — they are never
    inserted into leads regardless of their lead score.

    Args:
        now: UTC timestamp for created_at / updated_at.
    """
    print("\nSyncing leads table...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO leads (
                npi, first_name, last_name, full_name, credential,
                specialty, specialty_category, organization_name,
                practice_domain,
                email,
                personal_email, personal_email_confidence,
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
                p.npi,
                p.first_name_clean,
                p.last_name_clean,
                p.full_name_display,
                p.credential_normalized,
                p.specialty_name,
                p.derived_specialty_category,
                p.organization_name,
                p.practice_domain,
                -- legacy email column
                p.email,
                -- canonical email columns
                p.personal_email,
                p.personal_email_confidence,
                p.practice_email,
                p.email_confidence_score,
                p.email_confidence_level,
                p.email_verification_status,
                p.email_source,
                ppl.address_line_1,
                ppl.city,
                ppl.state,
                ppl.zip,
                p.lead_score_current,
                p.lead_tier,
                p.years_of_experience,
                p.experience_bucket,
                p.license_count,
                p.multi_state_flag,
                p.mobile_phone,
                p.phone_confidence,
                p.contact_completeness,
                -- compute contact_category inline:
                -- A = phone + any email
                -- B = any email, no phone
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
                    ELSE NULL  -- should never reach here due to WHERE clause
                END,
                p.enrichment_sources,
                :now,
                :now
            FROM physician p
            LEFT JOIN physician_practice_locations ppl
                ON p.npi = ppl.npi
               AND ppl.is_primary_location = TRUE
            WHERE (
                -- must have at least one email signal to enter leads
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

        result    = conn.execute(text("SELECT COUNT(*) FROM leads"))
        count_row = result.fetchone()
        count     = int(count_row[0]) if count_row and count_row[0] else 0
        print(f"  Leads table now has {count} records")

        # Breakdown by contact_category
        cat_result = conn.execute(text("""
            SELECT contact_category, COUNT(*) as cnt
            FROM leads
            GROUP BY contact_category
            ORDER BY contact_category
        """))
        for cat_row in cat_result.fetchall():
            print(f"  Category {cat_row[0]}: {cat_row[1]} leads")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_enrichment(
    limit: Optional[int] = None,
    npi:   Optional[str] = None,
) -> None:
    """
    Main entry point — orchestrates the full Hunter.io enrichment flow.

    Per-physician flow:
        1. Pass 1  — Hunter lookup by company name
        2. Store domain if returned (even on low score)
        3. Pass 2  — if score < 70, retry with domain directly
        4. Pre-filters — 5-layer free verification
        5. Assign confidence level
        6. Save email + update enrichment_sources + update stats
    After all physicians:
        7. Sync leads table with new contact_category values

    Args:
        limit: Max physicians to process (25 for Hunter free tier).
        npi:   Process single physician by NPI (test mode).
    """
    now = datetime.now(timezone.utc)

    print("=" * 60)
    print("PHYSICIAN EMAIL ENRICHMENT — Hunter.io")
    print(f"Started : {now.isoformat()}")
    if limit:
        print(f"Limit   : {limit} physicians")
    print("=" * 60)

    if not HUNTER_API_KEY:
        print("ERROR: HUNTER_API_KEY not set in .env")
        return

    physicians = get_physicians_to_enrich(limit=limit, npi=npi)
    if not physicians:
        print("No physicians to enrich.")
        return

    total          = len(physicians)
    hunter_found   = 0
    hunter_not_found = 0
    prefilter_failed = 0
    emails_saved   = 0
    pass2_upgrades = 0
    errors         = 0

    print(f"\nProcessing {total} physicians...\n" + "-" * 60)

    for i, row in enumerate(physicians, 1):
        npi_val       = row[0]
        first_name    = row[1] or ""
        last_name     = row[2] or ""
        org_name      = row[3] or ""
        stored_domain = row[4]
        score         = row[5]
        tier          = row[6]

        print(f"\n[{i}/{total}] {first_name} {last_name} | {org_name} | Score: {score} {tier}")

        # ── Pass 1 — company name ──────────────────────────────────────────
        print("  → Pass 1: searching by company name...")
        pass1 = call_hunter(first_name, last_name, company=org_name)
        time.sleep(HUNTER_RATE_LIMIT_DELAY)

        if not pass1["success"]:
            error = str(pass1.get("error", "unknown"))
            print(f"  → Pass 1 error: {error}")
            if error == "rate_limited":
                print("  → Rate limited — waiting 60s...")
                time.sleep(60)
            errors += 1
            mark_enrichment_failed(npi_val, f"hunter_error_{error}", now)
            continue

        email    = pass1.get("email")
        p1_score = int(pass1.get("score", 0))
        p1_domain = pass1.get("domain") or stored_domain or ""
        p1_verify = pass1.get("verification_status")

        if p1_domain:
            store_domain(npi_val, p1_domain, now)

        print(f"  → Pass 1: email={email} score={p1_score} domain={p1_domain}")

        # ── Pass 2 — direct domain (if score below threshold) ─────────────
        final_email  = email
        final_score  = p1_score
        final_domain = p1_domain
        final_verify = p1_verify
        pass_used    = 1

        if p1_domain and (not email or p1_score < MIN_SCORE_HIGH_CONFIDENCE):
            print(f"  → Score {p1_score} < {MIN_SCORE_HIGH_CONFIDENCE} — running Pass 2 with domain...")
            pass2 = call_hunter(first_name, last_name, domain=p1_domain)
            time.sleep(HUNTER_RATE_LIMIT_DELAY)

            if pass2["success"]:
                p2_email  = pass2.get("email")
                p2_score  = int(pass2.get("score", 0))
                p2_verify = pass2.get("verification_status")
                print(f"  → Pass 2: email={p2_email} score={p2_score}")

                if p2_email and p2_score > final_score:
                    final_email  = p2_email
                    final_score  = p2_score
                    final_verify = p2_verify
                    pass_used    = 2
                    pass2_upgrades += 1
                    print(f"  → Pass 2 UPGRADED: {p2_email} (score {p2_score})")
            else:
                print(f"  → Pass 2 error: {pass2.get('error')}")

        # ── Discard if no usable result ────────────────────────────────────
        if not final_email or final_score < MIN_SCORE_TO_PROCEED:
            reason = "no_result" if not final_email else f"low_score_{final_score}"
            print(f"  → Discarding: {reason}")
            hunter_not_found += 1
            mark_enrichment_failed(npi_val, reason, now)
            continue

        hunter_found += 1

        # ── Free pre-filters ───────────────────────────────────────────────
        print(f"  → Running pre-filters on {final_email}...")
        filter_result = run_free_prefilters(final_email)

        if not filter_result["passed"]:
            reason = str(filter_result["reason"])
            print(f"  → Pre-filter FAILED: {reason}")
            prefilter_failed += 1
            mark_enrichment_failed(npi_val, f"prefilter_{reason}", now)
            continue

        is_catch_all = bool(filter_result.get("is_catch_all", False))
        print(f"  → Pre-filters PASSED (catch-all: {is_catch_all})")

        # ── Assign confidence level ────────────────────────────────────────
        if final_verify == "valid":
            confidence_level    = "HIGH"
            final_verify_status = "hunter_verified"
        elif final_score >= MIN_SCORE_HIGH_CONFIDENCE and not is_catch_all:
            confidence_level    = "HIGH"
            final_verify_status = "pre_filter_passed"
        elif is_catch_all:
            confidence_level    = "LOW"
            final_verify_status = "catch_all_domain"
        else:
            confidence_level    = "MEDIUM"
            final_verify_status = "pre_filter_passed_low_score"

        print(f"  → Confidence: {confidence_level} | Status: {final_verify_status}")

        # ── Save to DB ─────────────────────────────────────────────────────
        save_email_to_db(
            npi=npi_val,
            email=final_email,
            score=final_score,
            domain=final_domain,
            verification_status=final_verify_status,
            confidence_level=confidence_level,
            pass_number=pass_used,
            now=now,
        )
        emails_saved += 1
        print("  → SAVED.")

    # ── Sync leads table ───────────────────────────────────────────────────
    sync_to_leads_table(now)

    # ── Summary ────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("ENRICHMENT COMPLETE — Hunter.io")
    print(f"  Total processed  : {total}")
    print(f"  Hunter found     : {hunter_found}")
    print(f"  Pass 2 upgrades  : {pass2_upgrades}")
    print(f"  Hunter not found : {hunter_not_found}")
    print(f"  Pre-filter failed: {prefilter_failed}")
    print(f"  Emails saved     : {emails_saved}")
    print(f"  Errors           : {errors}")
    if total > 0:
        print(f"  Success rate     : {emails_saved / total * 100:.1f}%")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Physician Email Enrichment — Hunter.io"
    )
    parser.add_argument("--limit", type=int,
                        help="Max physicians to process (default: all eligible)")
    parser.add_argument("--npi",   type=str,
                        help="Process a single physician by NPI (test mode)")
    args = parser.parse_args()
    run_enrichment(limit=args.limit, npi=args.npi)