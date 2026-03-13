# etl/enrich_emails.py
# Email enrichment pipeline using Hunter.io + free pre-filters.
# Two-pass strategy:
#   Pass 1: company name → email + domain + score
#   Pass 2: if score < 70, retry with domain for better result
#
# Usage:
#   python etl/enrich_emails.py --limit 25
#   python etl/enrich_emails.py --npi 1234567890

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

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")
HUNTER_EMAIL_FINDER_URL = "https://api.hunter.io/v2/email-finder"

MIN_SCORE_TO_PROCEED = 40
MIN_SCORE_HIGH_CONFIDENCE = 70
HUNTER_RATE_LIMIT_DELAY = 1.5


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


# ── STEP 1: PULL PHYSICIANS ───────────────────────────────────────────────────

def get_physicians_to_enrich(
    limit: Optional[int] = None,
    npi: Optional[str] = None
):
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
                WHERE email IS NULL
                  AND organization_name IS NOT NULL
                  AND is_active = TRUE
                  AND (email_enrichment_attempted IS NULL
                       OR email_enrichment_attempted = FALSE)
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
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def check_domain_exists(domain: str) -> bool:
    try:
        dns.resolver.resolve(domain, 'A')
        return True
    except Exception:
        return False


def check_mx_record(domain: str) -> bool:
    try:
        dns.resolver.resolve(domain, 'MX')
        return True
    except Exception:
        return False


def check_catch_all(domain: str) -> bool:
    try:
        random_email = f"randomtest_{uuid.uuid4().hex[:8]}@{domain}"
        mx_records = dns.resolver.resolve(domain, 'MX')
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
    'spam4.me', 'dispostable.com', 'mailnull.com'
}


def check_disposable(domain: str) -> bool:
    return domain.lower() in DISPOSABLE_DOMAINS


def run_free_prefilters(email: str) -> PreFilterResult:
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
        "passed": True,
        "reason": "all_checks_passed",
        "is_catch_all": is_catch_all,
        "domain": domain
    }


# ── STEP 3: HUNTER API ───────────────────────────────────────────────────────

def call_hunter(
    first_name: str,
    last_name: str,
    company: Optional[str] = None,
    domain: Optional[str] = None
) -> HunterResult:
    """
    Calls Hunter Email Finder.
    Pass 1: send company name
    Pass 2: send domain directly (higher accuracy)
    Must provide either company or domain.
    """
    params: dict[str, str] = {
        "first_name": first_name,
        "last_name": last_name,
        "api_key": HUNTER_API_KEY or "",
    }

    if domain:
        params["domain"] = domain        # Pass 2 — direct domain
    elif company:
        params["company"] = company      # Pass 1 — company name

    try:
        response = requests.get(
            HUNTER_EMAIL_FINDER_URL,
            params=params,
            timeout=15
        )

        if response.status_code == 200:
            data = cast(dict[str, Any], response.json())
            data_obj = cast(dict[str, Any], data.get("data", {}))
            verification_obj = cast(
                dict[str, Any],
                data_obj.get("verification") or {}
            )
            score_raw = data_obj.get("score", 0)
            score_val = int(score_raw) if isinstance(score_raw, (int, float)) else 0

            return {
                "success": True,
                "email": cast(Optional[str], data_obj.get("email")),
                "score": score_val,
                "domain": cast(Optional[str], data_obj.get("domain")),
                "verification_status": cast(
                    Optional[str], verification_obj.get("status")
                ),
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


# ── STEP 4: SAVE TO DATABASE ──────────────────────────────────────────────────

def save_email_to_db(
    npi: str,
    email: str,
    score: int,
    domain: str,
    verification_status: str,
    confidence_level: str,
    pass_number: int,
    now: datetime
):
    with engine.connect() as conn:

        conn.execute(text("""
            UPDATE physician SET
                email                      = :email,
                email_confidence_score     = :score,
                email_verification_status  = :verification_status,
                email_source               = 'hunter_io',
                email_confidence_level     = :confidence_level,
                email_acquired_at          = :now,
                practice_domain            = :domain,
                email_enrichment_attempted = TRUE,
                email_enrichment_result    = :result,
                updated_at                 = :now
            WHERE npi = :npi
        """), {
            "npi": npi,
            "email": email,
            "score": score,
            "verification_status": verification_status,
            "confidence_level": confidence_level,
            "domain": domain,
            "result": f"found_pass{pass_number}",
            "now": now,
        })

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
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE physician SET
                email_enrichment_attempted = TRUE,
                email_enrichment_result    = :reason,
                updated_at                 = :now
            WHERE npi = :npi
        """), {"npi": npi, "reason": reason, "now": now})
        conn.commit()


def store_domain(npi: str, domain: str, now: datetime):
    """Stores domain even if email wasn't found — useful for Pass 2 later."""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE physician SET
                practice_domain = :domain,
                updated_at      = :now
            WHERE npi = :npi
              AND practice_domain IS NULL
        """), {"npi": npi, "domain": domain, "now": now})
        conn.commit()


# ── STEP 5: RE-SCORE ──────────────────────────────────────────────────────────

def rescore_physician(npi: str, confidence_level: str, now: datetime):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT lead_score_current FROM physician WHERE npi = :npi
        """), {"npi": npi})
        row = result.fetchone()
        if not row:
            return

        current_score = float(row[0] or 0)
        email_pts = 40 if confidence_level == "HIGH" else 20

        # Add email points on top of base score (pillars 2+3+4)
        new_score = min(100, current_score + email_pts)

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


# ── STEP 6: POPULATE LEADS TABLE ─────────────────────────────────────────────

def sync_to_leads_table(now: datetime):
    """
    Syncs all physicians with verified emails into the leads table.
    This is the actionable list — only physicians with confirmed emails.
    """
    print("\nSyncing verified emails to leads table...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO leads (
                npi,
                first_name,
                last_name,
                full_name,
                credential,
                specialty,
                specialty_category,
                organization_name,
                practice_domain,
                email,
                email_confidence_score,
                email_confidence_level,
                email_verification_status,
                email_source,
                address_line_1,
                city,
                state,
                zip,
                lead_score,
                lead_tier,
                years_of_experience,
                experience_bucket,
                license_count,
                multi_state_flag,
                created_at,
                updated_at
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
                p.email,
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
                :now,
                :now
            FROM physician p
            LEFT JOIN physician_practice_locations ppl
                ON p.npi = ppl.npi
                AND ppl.is_primary_location = TRUE
            WHERE p.email IS NOT NULL
              AND p.email_confidence_level IN ('HIGH', 'MEDIUM')
            ON CONFLICT (npi) DO UPDATE SET
                email                    = EXCLUDED.email,
                email_confidence_score   = EXCLUDED.email_confidence_score,
                email_confidence_level   = EXCLUDED.email_confidence_level,
                email_verification_status = EXCLUDED.email_verification_status,
                lead_score               = EXCLUDED.lead_score,
                lead_tier                = EXCLUDED.lead_tier,
                updated_at               = EXCLUDED.updated_at
        """), {"now": now})
        conn.commit()

        result = conn.execute(text("SELECT COUNT(*) FROM leads"))
        count_row = result.fetchone()
        if count_row is None:
            count = 0
        else:
            raw_count = count_row[0]
            count = int(raw_count) if raw_count is not None else 0
        print(f"  Leads table now has {count} records")


# ── MAIN RUNNER ───────────────────────────────────────────────────────────────

def run_enrichment(
    limit: Optional[int] = None,
    npi: Optional[str] = None
):
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

    physicians = get_physicians_to_enrich(limit=limit, npi=npi)

    if not physicians:
        print("No physicians to enrich.")
        return

    total = len(physicians)
    hunter_found = 0
    hunter_not_found = 0
    prefilter_failed = 0
    emails_saved = 0
    pass2_upgrades = 0
    errors = 0

    print(f"\nProcessing {total} physicians...\n")
    print("-" * 60)

    for i, row in enumerate(physicians, 1):
        npi_val    = row[0]
        first_name = row[1] or ""
        last_name  = row[2] or ""
        org_name   = row[3] or ""
        stored_domain = row[4]          # domain from previous run if any
        score      = row[5]
        tier       = row[6]

        print(f"\n[{i}/{total}] {first_name} {last_name} | {org_name} | Score: {score} {tier}")

        # ── PASS 1 — Company name ─────────────────────────
        print(f"  → Pass 1: searching by company name...")
        pass1 = call_hunter(first_name, last_name, company=org_name)
        time.sleep(HUNTER_RATE_LIMIT_DELAY)

        if not pass1["success"]:
            error = str(pass1.get("error", "unknown"))
            print(f"  → Pass 1 error: {error}")
            if error == "rate_limited":
                print("  → Rate limited. Waiting 60s...")
                time.sleep(60)
            errors += 1
            mark_enrichment_failed(npi_val, f"hunter_error_{error}", now)
            continue

        email      = pass1.get("email")
        p1_score   = int(pass1.get("score", 0))
        p1_domain  = pass1.get("domain") or stored_domain or ""
        p1_verify  = pass1.get("verification_status")

        # Store domain even if score is low — useful for Pass 2
        if p1_domain:
            store_domain(npi_val, p1_domain, now)

        print(f"  → Pass 1 result: email={email} score={p1_score} domain={p1_domain}")

        # ── PASS 2 — Retry with domain if score is low ────
        final_email  = email
        final_score  = p1_score
        final_domain = p1_domain
        final_verify = p1_verify
        pass_used    = 1

        if p1_domain and (not email or p1_score < MIN_SCORE_HIGH_CONFIDENCE):
            print(f"  → Pass 1 score {p1_score} < {MIN_SCORE_HIGH_CONFIDENCE}. Running Pass 2 with domain...")
            pass2 = call_hunter(first_name, last_name, domain=p1_domain)
            time.sleep(HUNTER_RATE_LIMIT_DELAY)

            if pass2["success"]:
                p2_email  = pass2.get("email")
                p2_score  = int(pass2.get("score", 0))
                p2_verify = pass2.get("verification_status")

                print(f"  → Pass 2 result: email={p2_email} score={p2_score}")

                # Use Pass 2 result if it's better
                if p2_email and p2_score > final_score:
                    final_email  = p2_email
                    final_score  = p2_score
                    final_verify = p2_verify
                    pass_used    = 2
                    pass2_upgrades += 1
                    print(f"  → Pass 2 UPGRADED result: {p2_email} (score {p2_score})")
            else:
                print(f"  → Pass 2 error: {pass2.get('error')}")

        # ── DISCARD if still no good email ───────────────
        if not final_email or final_score < MIN_SCORE_TO_PROCEED:
            reason = "no_result" if not final_email else f"low_score_{final_score}"
            print(f"  → Discarding: {reason}")
            hunter_not_found += 1
            mark_enrichment_failed(npi_val, reason, now)
            continue

        hunter_found += 1

        # ── FREE PRE-FILTERS ──────────────────────────────
        print(f"  → Running free pre-filters on {final_email}...")
        filter_result = run_free_prefilters(final_email)

        if not filter_result["passed"]:
            reason = str(filter_result["reason"])
            print(f"  → Pre-filter FAILED: {reason}")
            prefilter_failed += 1
            mark_enrichment_failed(npi_val, f"prefilter_{reason}", now)
            continue

        is_catch_all = bool(filter_result.get("is_catch_all", False))
        print(f"  → Pre-filters PASSED (catch-all: {is_catch_all})")

        # ── CONFIDENCE LEVEL ──────────────────────────────
        if final_verify == "valid":
            confidence_level  = "HIGH"
            final_verify_status = "hunter_verified"
        elif final_score >= MIN_SCORE_HIGH_CONFIDENCE and not is_catch_all:
            confidence_level  = "HIGH"
            final_verify_status = "pre_filter_passed"
        elif is_catch_all:
            confidence_level  = "LOW"
            final_verify_status = "catch_all_domain"
        else:
            confidence_level  = "MEDIUM"
            final_verify_status = "pre_filter_passed_low_score"

        print(f"  → Confidence: {confidence_level} | Verification: {final_verify_status}")

        # ── SAVE ──────────────────────────────────────────
        save_email_to_db(
            npi=npi_val,
            email=final_email,
            score=final_score,
            domain=final_domain,
            verification_status=final_verify_status,
            confidence_level=confidence_level,
            pass_number=pass_used,
            now=now
        )

        rescore_physician(npi_val, confidence_level, now)
        emails_saved += 1
        print(f"  → SAVED. Score boosted by email confidence.")

    # ── SYNC TO LEADS TABLE ───────────────────────────────
    sync_to_leads_table(now)

    # ── SUMMARY ───────────────────────────────────────────
    print()
    print("=" * 60)
    print("ENRICHMENT COMPLETE")
    print(f"  Total processed:     {total}")
    print(f"  Hunter found:        {hunter_found}")
    print(f"  Pass 2 upgrades:     {pass2_upgrades}")
    print(f"  Hunter not found:    {hunter_not_found}")
    print(f"  Pre-filter failed:   {prefilter_failed}")
    print(f"  Emails saved:        {emails_saved}")
    print(f"  Errors:              {errors}")
    if total > 0:
        print(f"  Success rate:        {emails_saved/total*100:.1f}%")
    print("=" * 60)


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Physician Email Enrichment Pipeline"
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument("--npi", type=str)
    args = parser.parse_args()
    run_enrichment(limit=args.limit, npi=args.npi)