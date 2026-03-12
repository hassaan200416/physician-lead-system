# etl/enrich_emails.py
# Email enrichment pipeline using Hunter.io + free pre-filters.
# Finds emails for physicians using organization name + full name.
#
# Usage:
#   python etl/enrich_emails.py             (process all physicians without email)
#   python etl/enrich_emails.py --limit 25  (process first 25, good for free tier testing)
#   python etl/enrich_emails.py --npi 1234567890 (process single physician)

import sys
import time
import argparse
import re
import dns.resolver
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

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")
HUNTER_EMAIL_FINDER_URL = "https://api.hunter.io/v2/email-finder"

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

MIN_SCORE_TO_PROCEED = 40       # Discard Hunter results below this score
MIN_SCORE_HIGH_CONFIDENCE = 70  # Above this = high confidence, skip NeverBounce
HUNTER_RATE_LIMIT_DELAY = 1.5  # Seconds between Hunter calls (free tier safe)


class PreFilterResult(TypedDict):
    passed: bool
    reason: str
    is_catch_all: NotRequired[bool]
    domain: NotRequired[str]


class HunterResult(TypedDict):
    success: bool
    email: NotRequired[Optional[str]]
    score: NotRequired[int]
    domain: NotRequired[Optional[str]]
    verification_status: NotRequired[Optional[str]]
    error: NotRequired[str]


# ── STEP 1: PULL PHYSICIANS FROM DB ───────────────────────────────────────────

def get_physicians_to_enrich(limit: Optional[int] = None, npi: Optional[str] = None):
    """
    Pulls physicians that:
    - Have no email yet
    - Have an organization name (required for Hunter)
    - Are active
    Orders by lead_score_current DESC (best leads first)
    """
    with engine.connect() as conn:
        if npi:
            result = conn.execute(text("""
                SELECT 
                    npi,
                    first_name_clean,
                    last_name_clean,
                    organization_name,
                    lead_score_current,
                    lead_tier
                FROM physician
                WHERE npi = :npi
            """), {"npi": npi})
        else:
            query = """
                SELECT 
                    npi,
                    first_name_clean,
                    last_name_clean,
                    organization_name,
                    lead_score_current,
                    lead_tier
                FROM physician
                WHERE email IS NULL
                  AND organization_name IS NOT NULL
                  AND is_active = TRUE
                ORDER BY lead_score_current DESC
            """
            if limit:
                query += f" LIMIT {limit}"
            result = conn.execute(text(query))

        rows = result.fetchall()
        print(f"Found {len(rows)} physicians to enrich")
        return rows


# ── STEP 2: FREE PRE-FILTERS ──────────────────────────────────────────────────

def check_syntax(email: str) -> bool:
    """Basic email syntax check."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def check_domain_exists(domain: str) -> bool:
    """Check if domain has a DNS A record."""
    try:
        dns.resolver.resolve(domain, 'A')
        return True
    except Exception:
        return False


def check_mx_record(domain: str) -> bool:
    """Check if domain has MX records (can receive email)."""
    try:
        dns.resolver.resolve(domain, 'MX')
        return True
    except Exception:
        return False


def check_catch_all(domain: str) -> bool:
    """
    Check if domain accepts all emails (catch-all).
    Sends a random email — if accepted, domain is catch-all.
    Returns True if catch-all (meaning verification is unreliable).
    """
    try:
        import smtplib
        import uuid
        random_email = f"randomtest_{uuid.uuid4().hex[:8]}@{domain}"
        mx_records = dns.resolver.resolve(domain, 'MX')
        first_record = next(iter(mx_records), None)
        if first_record is None:
            return False
        # Typical MX record format: "10 mx.example.com."; host is last token.
        mx_host = str(first_record).split()[-1].rstrip('.')

        with smtplib.SMTP(timeout=5) as smtp:
            smtp.connect(mx_host)
            smtp.ehlo()
            smtp.mail('')
            code, _ = smtp.rcpt(random_email)
            return code == 250  # 250 = accepted = catch-all
    except Exception:
        return False  # If we can't check, assume not catch-all


DISPOSABLE_DOMAINS = {
    'mailinator.com', 'tempmail.com', 'throwaway.email',
    'guerrillamail.com', 'yopmail.com', 'trashmail.com',
    'sharklasers.com', 'guerrillamailblock.com', 'grr.la',
    'spam4.me', 'dispostable.com', 'mailnull.com'
}


def check_disposable(domain: str) -> bool:
    """Check if domain is a known disposable email provider."""
    return domain.lower() in DISPOSABLE_DOMAINS


def run_free_prefilters(email: str) -> PreFilterResult:
    """
    Runs all free pre-filters in order.
    Returns result dict with passed/failed status and reason.
    Stops at first failure to save time.
    """
    domain = email.split('@')[1] if '@' in email else ''

    # Check 1 — Syntax
    if not check_syntax(email):
        return {"passed": False, "reason": "invalid_syntax"}

    # Check 2 — Disposable domain
    if check_disposable(domain):
        return {"passed": False, "reason": "disposable_domain"}

    # Check 3 — Domain exists
    if not check_domain_exists(domain):
        return {"passed": False, "reason": "domain_not_found"}

    # Check 4 — MX record exists
    if not check_mx_record(domain):
        return {"passed": False, "reason": "no_mx_record"}

    # Check 5 — Catch-all detection
    is_catch_all = check_catch_all(domain)

    return {
        "passed": True,
        "reason": "all_checks_passed",
        "is_catch_all": is_catch_all,
        "domain": domain
    }


# ── STEP 3: HUNTER.IO API CALL ────────────────────────────────────────────────

def call_hunter(first_name: str, last_name: str, company: str) -> HunterResult:
    """
    Calls Hunter.io Email Finder API.
    Returns the full response dict.
    """
    params: dict[str, str] = {
        "first_name": first_name,
        "last_name": last_name,
        "company": company,
        "api_key": HUNTER_API_KEY or "",
    }

    try:
        response = requests.get(
            HUNTER_EMAIL_FINDER_URL,
            params=params,
            timeout=15
        )

        if response.status_code == 200:
            data = cast(dict[str, Any], response.json())
            data_obj = cast(dict[str, Any], data.get("data", {}))
            verification_obj = cast(dict[str, Any], data_obj.get("verification", {}))
            score_raw = data_obj.get("score", 0)
            score_val = 0
            if isinstance(score_raw, (int, float)):
                score_val = int(score_raw)
            elif isinstance(score_raw, str):
                try:
                    score_val = int(score_raw)
                except ValueError:
                    score_val = 0
            return {
                "success": True,
                "email": cast(Optional[str], data_obj.get("email")),
                "score": score_val,
                "domain": cast(Optional[str], data_obj.get("domain")),
                "verification_status": cast(Optional[str], verification_obj.get("status")),
            }

        elif response.status_code == 401:
            return {"success": False, "error": "invalid_api_key"}

        elif response.status_code == 429:
            return {"success": False, "error": "rate_limited"}

        else:
            return {
                "success": False,
                "error": f"http_{response.status_code}"
            }

    except requests.exceptions.Timeout:
        return {"success": False, "error": "timeout"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── STEP 4: WRITE RESULT TO DATABASE ──────────────────────────────────────────

def save_email_to_db(
    npi: str,
    email: str,
    score: int,
    domain: str,
    verification_status: str,
    confidence_level: str,
    now: datetime
):
    """
    Writes verified email back to physician table.
    Also stores domain for future use.
    Also writes to field_value_history for audit trail.
    """
    with engine.connect() as conn:
        # Update physician table
        conn.execute(text("""
            UPDATE physician SET
                email                      = :email,
                email_confidence_score     = :score,
                email_verification_status  = :verification_status,
                email_source               = 'hunter_io',
                email_confidence_level     = :confidence_level,
                email_acquired_at          = :now,
                practice_domain            = :domain,
                updated_at                 = :now
            WHERE npi = :npi
        """), {
            "npi": npi,
            "email": email,
            "score": score,
            "verification_status": verification_status,
            "confidence_level": confidence_level,
            "domain": domain,
            "now": now,
        })

        # Write to field_value_history for audit trail
        conn.execute(text("""
    INSERT INTO field_value_history (
        history_id, entity_type, entity_id, npi, field_name,
        field_value, source_name,
        confidence_score, is_current,
        collected_timestamp, created_at
    ) VALUES (
        gen_random_uuid(), 'physician', :npi, :npi, 'email',
        :email, 'hunter_io',
        :score, TRUE,
        :now, :now
    )
"""), {
    "npi": npi,
    "email": email,
    "score": score / 100,
    "now": now,
})

        conn.commit()


def mark_enrichment_failed(npi: str, reason: str, now: datetime):
    """
    Marks a physician as enrichment attempted but failed.
    Prevents re-processing on next run.
    """
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE physician SET
                email_enrichment_attempted = TRUE,
                email_enrichment_result    = :reason,
                updated_at                 = :now
            WHERE npi = :npi
        """), {"npi": npi, "reason": reason, "now": now})
        conn.commit()


# ── STEP 5: RE-SCORE AFTER EMAIL ADDED ────────────────────────────────────────

def rescore_physician(npi: str, now: datetime):
    """
    Recalculates lead score after email is added.
    Email adds 5pts to Pillar 1.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT lead_score_current FROM physician WHERE npi = :npi
        """), {"npi": npi})
        row = result.fetchone()
        if not row:
            return

        current_score = float(row[0] or 0)
        new_score = min(100, current_score + 5)

        # Recalculate tier
        if new_score >= 80:
            new_tier = 'A'
        elif new_score >= 60:
            new_tier = 'B'
        elif new_score >= 40:
            new_tier = 'C'
        else:
            new_tier = 'Archive'

        conn.execute(text("""
            UPDATE physician SET
                lead_score_current      = :score,
                lead_tier               = :tier,
                lead_score_last_updated = :now
            WHERE npi = :npi
        """), {"npi": npi, "score": new_score, "tier": new_tier, "now": now})
        conn.commit()


# ── MAIN ENRICHMENT RUNNER ────────────────────────────────────────────────────

def run_enrichment(limit: Optional[int] = None, npi: Optional[str] = None):
    """
    Main enrichment function.
    Processes physicians through Hunter + pre-filters pipeline.
    """
    now = datetime.now(timezone.utc)

    print("=" * 60)
    print("PHYSICIAN EMAIL ENRICHMENT — Hunter.io Pipeline")
    print(f"Started: {now.isoformat()}")
    if limit:
        print(f"Limit: {limit} physicians")
    print("=" * 60)

    if not HUNTER_API_KEY:
        print("ERROR: HUNTER_API_KEY not found in .env file")
        return

    # Pull physicians to enrich
    physicians = get_physicians_to_enrich(limit=limit, npi=npi)

    if not physicians:
        print("No physicians to enrich.")
        return

    # Counters
    total = len(physicians)
    hunter_found = 0
    hunter_not_found = 0
    prefilter_failed = 0
    emails_saved = 0
    errors = 0

    print(f"\nProcessing {total} physicians...\n")
    print("-" * 60)

    for i, row in enumerate(physicians, 1):
        npi_val = row[0]
        first_name = row[1] or ""
        last_name = row[2] or ""
        org_name = row[3] or ""
        score = row[4]
        tier = row[5]

        print(f"[{i}/{total}] {first_name} {last_name} | {org_name} | Score: {score} {tier}")

        # ── HUNTER API CALL ───────────────────────────────
        hunter_result = call_hunter(first_name, last_name, org_name)

        # Rate limiting — stay within Hunter's limits
        time.sleep(HUNTER_RATE_LIMIT_DELAY)

        if not hunter_result["success"]:
            error = str(hunter_result.get("error", "unknown"))
            print(f"  → Hunter error: {error}")

            if error == "rate_limited":
                print("  → Rate limited. Waiting 60 seconds...")
                time.sleep(60)

            errors += 1
            mark_enrichment_failed(npi_val, f"hunter_error_{error}", now)
            continue

        email = hunter_result.get("email")
        hunter_score = int(hunter_result.get("score", 0))
        domain = hunter_result.get("domain") or ""
        verification_status = hunter_result.get("verification_status")

        # No email found
        if not email or hunter_score < MIN_SCORE_TO_PROCEED:
            reason = "hunter_no_result" if not email else f"hunter_low_score_{hunter_score}"
            print(f"  → Hunter: no email found (score: {hunter_score})")
            hunter_not_found += 1
            mark_enrichment_failed(npi_val, reason, now)
            continue

        hunter_found += 1
        print(f"  → Hunter found: {email} (score: {hunter_score}, domain: {domain})")

        # ── FREE PRE-FILTERS ──────────────────────────────
        print(f"  → Running pre-filters...")
        filter_result = run_free_prefilters(email)

        if not filter_result["passed"]:
            reason = str(filter_result["reason"])
            print(f"  → Pre-filter FAILED: {reason}")
            prefilter_failed += 1
            mark_enrichment_failed(npi_val, f"prefilter_{reason}", now)
            continue

        is_catch_all = bool(filter_result.get("is_catch_all", False))
        print(f"  → Pre-filters PASSED (catch-all: {is_catch_all})")

        # ── DETERMINE CONFIDENCE LEVEL ────────────────────
        # Hunter already verified on paid tier
        if verification_status == "valid":
            confidence_level = "HIGH"
            final_verification = "hunter_verified"
            print(f"  → Hunter verified: VALID")

        # High score + not catch-all = high confidence
        elif hunter_score >= MIN_SCORE_HIGH_CONFIDENCE and not is_catch_all:
            confidence_level = "HIGH"
            final_verification = "pre_filter_passed"
            print(f"  → Confidence: HIGH (score {hunter_score}, not catch-all)")

        # Catch-all domain
        elif is_catch_all:
            confidence_level = "LOW"
            final_verification = "catch_all_domain"
            print(f"  → Confidence: LOW (catch-all domain)")

        # Low score but passed filters
        else:
            confidence_level = "MEDIUM"
            final_verification = "pre_filter_passed_low_score"
            print(f"  → Confidence: MEDIUM (score {hunter_score})")

        # ── SAVE TO DATABASE ──────────────────────────────
        save_email_to_db(
            npi=npi_val,
            email=email,
            score=hunter_score,
            domain=domain,
            verification_status=final_verification,
            confidence_level=confidence_level,
            now=now
        )

        # ── RE-SCORE PHYSICIAN ────────────────────────────
        rescore_physician(npi_val, now)

        emails_saved += 1
        print(f"  → SAVED. Score +5pts applied.")

    # ── SUMMARY ───────────────────────────────────────────
    print()
    print("=" * 60)
    print("ENRICHMENT COMPLETE")
    print(f"  Total processed:     {total}")
    print(f"  Hunter found:        {hunter_found}")
    print(f"  Hunter not found:    {hunter_not_found}")
    print(f"  Pre-filter failed:   {prefilter_failed}")
    print(f"  Emails saved:        {emails_saved}")
    print(f"  Errors:              {errors}")
    print(f"  Success rate:        {emails_saved/total*100:.1f}%")
    print("=" * 60)


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Physician Email Enrichment Pipeline"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of physicians to process"
    )
    parser.add_argument(
        "--npi",
        type=str,
        help="Process a single physician by NPI"
    )
    args = parser.parse_args()
    run_enrichment(limit=args.limit, npi=args.npi)
