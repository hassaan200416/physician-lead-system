# etl/enrich_phones.py
# Phone enrichment pipeline using PeopleDataLabs + Twilio + Telnyx.
# Two-pass strategy:
#   Pass 1: NPI lookup in PDL (highest precision)
#   Pass 2: name + location fallback (stricter likelihood)
#
# Usage:
#   python etl/enrich_phones.py --npi 1629071840
#   python etl/enrich_phones.py --limit 25
#   python etl/enrich_phones.py

import os
import sys
import argparse
import logging
import importlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import SessionLocal
from etl.compute_scores import (
    compute_contact_completeness,
    compute_reachability_score,
    get_state_risk,
)


load_dotenv()

PDL_API_KEY = os.getenv("PDL_API_KEY")
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TELNYX_KEY = os.getenv("TELNYX_API_KEY")

PDL_ENDPOINT = "https://api.peopledatalabs.com/v5/person/enrich"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def _pdl_query(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    PDL requires a GET request with flat query parameters.
    Not a POST with nested params object.
    """
    if not PDL_API_KEY:
        log.warning("PDL_API_KEY not configured")
        return None

    try:
        # Flatten the nested params into top-level query params
        params = payload.get("params", {})
        params["api_key"] = PDL_API_KEY

        resp = requests.get(
            PDL_ENDPOINT,
            params=params,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data
        if resp.status_code == 404:
            return None

        log.warning("PDL %s: %s", resp.status_code, resp.text[:300])
        return None
    except requests.RequestException as exc:
        log.error("PDL request error: %s", exc)
        return None


def _parse_pdl_result(result: dict[str, Any], pass_used: int) -> Optional[dict[str, Any]]:
    """
    Normalize PDL response fields.
    mobile_phone in PDL can be a boolean flag (True/False)
    or an actual number string. The real numbers are in phone_numbers list.
    """
    data = result.get("data", {})
    likelihood_raw = result.get("likelihood", 0)

    try:
        likelihood = float(likelihood_raw)
    except (TypeError, ValueError):
        likelihood = 0.0

    # phone_numbers is the reliable list - always use this
    all_phones = data.get("phone_numbers")
    if not isinstance(all_phones, list):
        all_phones = []

    # mobile_phone field is sometimes a boolean in PDL - ignore if so
    mobile_raw = data.get("mobile_phone")
    if isinstance(mobile_raw, str) and mobile_raw.startswith("+"):
        mobile = mobile_raw
    else:
        # Fall back to first number in phone_numbers list
        mobile = all_phones[0] if all_phones else None

    # No usable phone number found
    if not mobile:
        return None

    if likelihood >= 8:
        confidence = "HIGH"
    elif likelihood >= 7:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "mobile_phone": mobile,
        "all_phones": all_phones,
        "pdl_likelihood": likelihood,
        "phone_confidence": confidence,
        "personal_emails": data.get("personal_emails") or [],
        "linkedin_url": data.get("linkedin_url"),
        "pass_used": pass_used,
    }


# State name to abbreviation mapping for PDL response normalization
_STATE_NAME_TO_ABBR = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct",
    "delaware": "de", "florida": "fl", "georgia": "ga", "hawaii": "hi",
    "idaho": "id", "illinois": "il", "indiana": "in", "iowa": "ia",
    "kansas": "ks", "kentucky": "ky", "louisiana": "la", "maine": "me",
    "maryland": "md", "massachusetts": "ma", "michigan": "mi",
    "minnesota": "mn", "mississippi": "ms", "missouri": "mo",
    "montana": "mt", "nebraska": "ne", "nevada": "nv",
    "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm",
    "new york": "ny", "north carolina": "nc", "north dakota": "nd",
    "ohio": "oh", "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa",
    "rhode island": "ri", "south carolina": "sc", "south dakota": "sd",
    "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt",
    "virginia": "va", "washington": "wa", "west virginia": "wv",
    "wisconsin": "wi", "wyoming": "wy",
}

_MEDICAL_KEYWORDS = {
    "medical", "health", "hospital", "clinic", "pharma",
    "physician", "doctor", "surgery", "dental", "therapy",
    "care", "oncology", "cardio", "neuro", "ortho", "pediatric",
    "psychiatric", "radiology", "dermatology", "practice",
}

_ORG_IGNORE_WORDS = {
    "the", "of", "and", "a", "an", "for", "medical", "health",
    "care", "center", "group", "associates", "practice", "clinic",
    "institute", "services", "solutions", "management",
}


def verify_pdl_match(
    pdl_result: dict[str, Any],
    our_physician: dict[str, Any],
) -> tuple[bool, str]:
    """
    Cross-references PDL response against our NPPES data
    to confirm we matched the right person.

    Returns (is_verified, reason).

    Rules:
    - FAIL  : hard contradiction found - definitely wrong person
    - PASS  : no contradictions found
    - MEDIUM: soft contradiction - org name mismatch only
              (physician may have moved practice)

    Missing data is never treated as a contradiction.
    Only explicit mismatches cause failure.
    """
    data = pdl_result.get("data", {}) if "data" in pdl_result else pdl_result

    # -- Layer 1 - Industry must be medical --------------------
    industry = (data.get("job_company_industry") or "").lower()
    industry_v2 = (data.get("job_company_industry_v2") or "").lower()
    combined = f"{industry} {industry_v2}".strip()

    if combined:
        is_medical = any(kw in combined for kw in _MEDICAL_KEYWORDS)
        if not is_medical:
            return False, f"Industry not medical: '{industry}'"

    # -- Layer 2 - State must match ----------------------------
    pdl_region = (
        data.get("job_company_location_region") or
        data.get("location_region") or ""
    ).lower().strip()

    our_state = (our_physician.get("location_state") or "").lower().strip()

    if pdl_region and our_state:
        # Normalize PDL full state name to abbreviation
        pdl_abbr = _STATE_NAME_TO_ABBR.get(pdl_region, pdl_region)
        if pdl_abbr != our_state:
            return False, f"State mismatch: PDL='{pdl_region}' ours='{our_state}'"

    # -- Layer 3 - Organization name similarity (soft) ---------
    pdl_company = (data.get("job_company_name") or "").lower()
    our_org = (our_physician.get("organization_name") or "").lower()

    if pdl_company and our_org:
        pdl_words = {word for word in pdl_company.split() if word not in _ORG_IGNORE_WORDS}
        our_words = {word for word in our_org.split() if word not in _ORG_IGNORE_WORDS}

        if pdl_words and our_words and not (pdl_words & our_words):
            # Soft fail - downgrade to MEDIUM, do not reject
            # Physician may have changed practice since PDL last updated
            return True, "MEDIUM_CONFIDENCE: org name differs but state+industry match"

    return True, "VERIFIED"


def enrich_via_pdl(
    npi: str,
    first_name: str,
    last_name: str,
    city: str,
    state: str,
    email: Optional[str] = None,
    practice_phone: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """
    Two-pass PDL enrichment.
    Pass 1: first_name + last_name + locality + region
    Pass 2: adds practice_phone or email as extra signal
    """
    # -- Pass 1 - name + locality + region ----------------------
    pass1 = _pdl_query({
        "params": {
            "first_name": first_name,
            "last_name": last_name,
            "locality": city,
            "region": state,
            "min_likelihood": 7,
        },
        # No required filter - get full record, check mobile after
    })

    if pass1 and float(pass1.get("likelihood", 0) or 0) >= 7:
        parsed = _parse_pdl_result(pass1, pass_used=1)
        if parsed:
            log.info("NPI [%s] - PDL pass 1 hit (likelihood=%s)",
                     npi, pass1.get("likelihood"))
            return parsed

    # -- Pass 2 - add phone or email as extra signal ------------
    extra: dict[str, Any] = {}
    if practice_phone:
        extra["phone"] = practice_phone
    elif email:
        extra["email"] = email

    if extra:
        pass2 = _pdl_query({
            "params": {
                "first_name": first_name,
                "last_name": last_name,
                "locality": city,
                "region": state,
                "min_likelihood": 8,
                **extra,
            },
        })

        if pass2 and float(pass2.get("likelihood", 0) or 0) >= 8:
            parsed = _parse_pdl_result(pass2, pass_used=2)
            if parsed:
                log.info("NPI [%s] - PDL pass 2 hit (likelihood=%s)",
                         npi, pass2.get("likelihood"))
                return parsed

    log.info("NPI [%s] - PDL no result on either pass", npi)
    return None


def validate_line_type(phone_number: str) -> dict[str, Any]:
    """Validate phone line type via Twilio Lookup API."""
    if not TWILIO_SID or not TWILIO_TOKEN:
        return {
            "is_valid": True,
            "line_type": "unknown",
            "carrier": "unknown",
            "is_mobile": False,
        }

    try:
        twilio_rest = importlib.import_module("twilio.rest")
        Client = getattr(twilio_rest, "Client")
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        lookup = client.lookups.v2.phone_numbers(phone_number).fetch(
            fields=["line_type_intelligence"]
        )

        line_intel = lookup.line_type_intelligence or {}
        line_type = line_intel.get("type", "unknown")

        return {
            "is_valid": bool(getattr(lookup, "valid", True)),
            "line_type": line_type,
            "carrier": line_intel.get("carrier_name", "unknown"),
            "is_mobile": line_type == "mobile",
        }
    except Exception as exc:
        log.error("Twilio lookup failed for %s: %s", phone_number, exc)
        return {
            "is_valid": True,
            "line_type": "unknown",
            "carrier": "unknown",
            "is_mobile": False,
        }


def check_dnc(phone_number: str) -> bool:
    """Check Telnyx DNC registry status; fail safe on errors."""
    if not TELNYX_KEY:
        return False

    try:
        resp = requests.get(
            f"https://api.telnyx.com/v2/number_lookup/{phone_number}",
            headers={
                "Authorization": f"Bearer {TELNYX_KEY}",
                "Content-Type": "application/json",
            },
            params={"type": "dnc"},
            timeout=10,
        )

        if resp.status_code == 200:
            dnc = resp.json().get("data", {}).get("dnc", {})
            on_dnc = any(
                [
                    dnc.get("federal_dnc", False),
                    dnc.get("state_dnc", False),
                    dnc.get("wireless_dnc", False),
                ]
            )
            return not on_dnc

        return False
    except Exception as exc:
        log.error("DNC check failed for %s: %s", phone_number, exc)
        return False


def _tier_from_score(score: int) -> str:
    if score >= 80:
        return "A"
    if score >= 60:
        return "B"
    if score >= 40:
        return "C"
    return "Archive"


def recalculate_score_after_phone(
    physician: dict[str, Any],
    previous_reachability: Optional[int] = None,
) -> dict[str, Any]:
    """Recalculate reachability and derived score/tier fields."""
    p1 = compute_reachability_score(
        mobile_phone=physician.get("mobile_phone"),
        phone_confidence=physician.get("phone_confidence"),
        phone_dnc_clear=physician.get("phone_dnc_clear"),
        personal_email=physician.get("email"),
        email_confidence=physician.get("email_confidence_level"),
        practice_email=physician.get("practice_email"),
    )

    old_total = int(float(physician.get("lead_score_current") or 0))
    prior_p1 = int(previous_reachability or 0)
    other_pillars = max(0, old_total - prior_p1)
    new_total = min(100, other_pillars + p1)

    completeness = compute_contact_completeness(
        has_mobile=bool(physician.get("mobile_phone") and physician.get("phone_dnc_clear")),
        has_personal_email=bool(physician.get("email")),
        has_practice_email=bool(physician.get("practice_email")),
    )

    return {
        "lead_score_current": new_total,
        "lead_tier": _tier_from_score(new_total),
        "reachability_score": p1,
        "contact_completeness": completeness,
    }


def sync_to_leads(npi: str, db: Session) -> None:
    """Upsert one physician record into the leads table."""
    now = datetime.now(timezone.utc)

    db.execute(
        text(
            """
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
                mobile_phone,
                phone_confidence,
                personal_email,
                personal_email_confidence,
                practice_email,
                contact_completeness,
                is_uncontactable,
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
                p.mobile_phone,
                p.phone_confidence,
                p.email,
                p.email_confidence_level,
                p.practice_email,
                p.contact_completeness,
                CASE WHEN p.contact_completeness = 'UNCONTACTABLE'
                     THEN TRUE ELSE FALSE END,
                :now,
                :now
            FROM physician p
            LEFT JOIN physician_practice_locations ppl
                ON p.npi = ppl.npi
                AND ppl.is_primary_location = TRUE
            WHERE p.npi = :npi
              AND p.lead_tier IN ('A', 'B', 'C')
            ON CONFLICT (npi) DO UPDATE SET
                mobile_phone               = EXCLUDED.mobile_phone,
                phone_confidence           = EXCLUDED.phone_confidence,
                personal_email             = EXCLUDED.personal_email,
                personal_email_confidence  = EXCLUDED.personal_email_confidence,
                practice_email             = EXCLUDED.practice_email,
                contact_completeness       = EXCLUDED.contact_completeness,
                is_uncontactable           = EXCLUDED.is_uncontactable,
                lead_score                 = EXCLUDED.lead_score,
                lead_tier                  = EXCLUDED.lead_tier,
                updated_at                 = EXCLUDED.updated_at
            """
        ),
        {"npi": npi, "now": now},
    )


def enrich_phones(npi_filter: Optional[str] = None, limit: Optional[int] = None) -> None:
    """Main phone enrichment runner."""
    db = SessionLocal()

    try:
        query_sql = """
            SELECT
                p.npi,
                p.first_name_clean,
                p.last_name_clean,
                p.email,
                p.email_confidence_level,
                p.practice_email,
                p.mobile_phone,
                p.phone_confidence,
                p.phone_dnc_clear,
                p.phone_enrichment_attempted,
                p.lead_tier,
                p.lead_score_current,
                ppl.city,
                ppl.state AS location_state
            FROM physician p
            LEFT JOIN physician_practice_locations ppl
                ON p.npi = ppl.npi
                AND ppl.is_primary_location = TRUE
            WHERE COALESCE(p.phone_enrichment_attempted, FALSE) = FALSE
              AND p.lead_tier IN ('A', 'B', 'C')
        """

        params: dict[str, Any] = {}
        if npi_filter:
            query_sql += " AND p.npi = :npi"
            params["npi"] = npi_filter

        query_sql += " ORDER BY p.lead_score_current DESC"

        if limit:
            query_sql += " LIMIT :limit"
            params["limit"] = limit

        rows = db.execute(text(query_sql), params).mappings().all()
        total = len(rows)
        log.info("Starting phone enrichment for %s physicians", total)

        found = 0
        mobile_confirmed = 0
        dnc_clear = 0
        pdl_bonus_emails = 0

        for idx, row in enumerate(rows, 1):
            physician = dict(row)
            npi = str(physician.get("npi") or "")
            first_name = str(physician.get("first_name_clean") or "")
            last_name = str(physician.get("last_name_clean") or "")
            city = str(physician.get("city") or "")
            state = str(physician.get("location_state") or "")

            old_p1 = compute_reachability_score(
                mobile_phone=physician.get("mobile_phone"),
                phone_confidence=physician.get("phone_confidence"),
                phone_dnc_clear=physician.get("phone_dnc_clear"),
                personal_email=physician.get("email"),
                email_confidence=physician.get("email_confidence_level"),
                practice_email=physician.get("practice_email"),
            )

            log.info("[%s/%s] NPI %s - %s %s", idx, total, npi, first_name, last_name)

            now = datetime.now(timezone.utc)
            db.execute(
                text(
                    """
                    UPDATE physician
                    SET phone_enrichment_attempted = TRUE,
                        phone_enriched_at = :now,
                        updated_at = :now
                    WHERE npi = :npi
                    """
                ),
                {"npi": npi, "now": now},
            )
            db.commit()

            pdl_result = enrich_via_pdl(
                npi=npi,
                first_name=first_name,
                last_name=last_name,
                city=city,
                state=state,
                email=physician.get("email"),
                practice_phone=physician.get("practice_phone"),
            )

            if not pdl_result or not pdl_result.get("mobile_phone"):
                continue

            # -- Verify correct person identity ------------------
            is_verified, verify_reason = verify_pdl_match(
                pdl_result=pdl_result,
                our_physician=physician,
            )

            if not is_verified:
                log.warning(
                    "NPI %s - identity verification FAILED: %s",
                    npi, verify_reason
                )
                continue

            if "MEDIUM_CONFIDENCE" in verify_reason:
                log.info("NPI %s - soft match: %s", npi, verify_reason)
                # Cap at MEDIUM even if PDL said HIGH
                if pdl_result.get("phone_confidence") == "HIGH":
                    pdl_result["phone_confidence"] = "MEDIUM"

            found += 1
            mobile = str(pdl_result["mobile_phone"])

            line_result = validate_line_type(mobile)
            db.execute(
                text(
                    """
                    UPDATE physician
                    SET phone_line_type = :line_type,
                        phone_carrier = :carrier,
                        updated_at = :now
                    WHERE npi = :npi
                    """
                ),
                {
                    "npi": npi,
                    "line_type": line_result.get("line_type", "unknown"),
                    "carrier": line_result.get("carrier", "unknown"),
                    "now": datetime.now(timezone.utc),
                },
            )
            db.commit()

            if not line_result.get("is_mobile", False):
                log.info("NPI %s - not mobile (%s), skipping", npi, line_result.get("line_type"))
                continue

            mobile_confirmed += 1

            clear = check_dnc(mobile)
            db.execute(
                text(
                    """
                    UPDATE physician
                    SET phone_dnc_checked = TRUE,
                        phone_dnc_clear = :clear,
                        updated_at = :now
                    WHERE npi = :npi
                    """
                ),
                {"npi": npi, "clear": clear, "now": datetime.now(timezone.utc)},
            )
            db.commit()

            if not clear:
                log.info("NPI %s - on DNC list, not storing", npi)
                continue

            dnc_clear += 1

            phone_conf = str(pdl_result.get("phone_confidence") or "LOW")
            bonus_emails = pdl_result.get("personal_emails") or []

            email_to_store = physician.get("email")
            email_conf = physician.get("email_confidence_level")
            if bonus_emails and not email_to_store:
                email_to_store = bonus_emails[0]
                email_conf = "MEDIUM"
                pdl_bonus_emails += 1
                log.info("NPI %s - bonus personal email from PDL", npi)

            db.execute(
                text(
                    """
                    UPDATE physician
                    SET mobile_phone = :mobile,
                        phone_confidence = :phone_confidence,
                        phone_state_risk = :state_risk,
                        email = COALESCE(:email_value, email),
                        email_confidence_level = COALESCE(:email_conf, email_confidence_level),
                        updated_at = :now
                    WHERE npi = :npi
                    """
                ),
                {
                    "npi": npi,
                    "mobile": mobile,
                    "phone_confidence": phone_conf,
                    "state_risk": get_state_risk(state),
                    "email_value": email_to_store,
                    "email_conf": email_conf,
                    "now": datetime.now(timezone.utc),
                },
            )
            db.commit()

            refreshed = db.execute(
                text(
                    """
                    SELECT
                        npi,
                        email,
                        email_confidence_level,
                        practice_email,
                        mobile_phone,
                        phone_confidence,
                        phone_dnc_clear,
                        lead_score_current
                    FROM physician
                    WHERE npi = :npi
                    """
                ),
                {"npi": npi},
            ).mappings().first()

            if refreshed:
                score_update = recalculate_score_after_phone(
                    dict(refreshed),
                    previous_reachability=old_p1,
                )
                db.execute(
                    text(
                        """
                        UPDATE physician
                        SET lead_score_current = :score,
                            lead_tier = :tier,
                            contact_completeness = :completeness,
                            updated_at = :now
                        WHERE npi = :npi
                        """
                    ),
                    {
                        "npi": npi,
                        "score": score_update["lead_score_current"],
                        "tier": score_update["lead_tier"],
                        "completeness": score_update["contact_completeness"],
                        "now": datetime.now(timezone.utc),
                    },
                )
                sync_to_leads(npi=npi, db=db)
                db.commit()

                log.info(
                    "NPI %s - stored mobile=%s confidence=%s completeness=%s",
                    npi,
                    mobile,
                    phone_conf,
                    score_update["contact_completeness"],
                )

        log.info("=" * 50)
        log.info("Phone enrichment complete")
        log.info("  Processed        : %s", total)
        log.info("  PDL found        : %s", found)
        log.info("  Mobile confirmed : %s", mobile_confirmed)
        log.info("  DNC clear        : %s", dnc_clear)
        log.info("  Bonus emails     : %s", pdl_bonus_emails)
        log.info("=" * 50)

    finally:
        db.close()


def preview_phone_coverage() -> None:
    """
    Uses PDL Preview API to check how many physicians
    actually have mobile phone data before purchasing.
    """
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT p.npi, p.first_name_clean, p.last_name_clean,
                   ppl.city, ppl.state AS location_state
            FROM physician p
            LEFT JOIN physician_practice_locations ppl
                ON p.npi = ppl.npi AND ppl.is_primary_location = TRUE
            WHERE p.lead_tier IN ('A', 'B', 'C')
            LIMIT 100
        """)).mappings().all()

        total = len(rows)
        has_mobile = 0
        has_email = 0
        found = 0
        not_found = 0
        errors = 0

        log.info("Running PDL preview on %s physicians...", total)

        for i, row in enumerate(rows, 1):
            first = row["first_name_clean"] or ""
            last = row["last_name_clean"] or ""
            city = row["city"] or ""
            state = row["location_state"] or ""

            if not first or not last:
                log.info("[%s/%s] Skipping - missing name", i, total)
                continue

            log.info("[%s/%s] Checking %s %s, %s %s",
                     i, total, first, last, city, state)

            try:
                resp = requests.get(
                    "https://api.peopledatalabs.com/v5/person/enrich",
                    params={
                        "api_key": PDL_API_KEY,
                        "first_name": first,
                        "last_name": last,
                        "locality": city,
                        "region": state,
                        "min_likelihood": 7,
                        "preview": "true",
                    },
                    timeout=10
                )

                log.info("  Status: %s", resp.status_code)

                if resp.status_code == 200:
                    found += 1
                    data = resp.json().get("data", {})
                    mobile = data.get("mobile_phone")
                    email = data.get("personal_emails")
                    log.info(
                        "  mobile_phone=%s personal_emails=%s",
                        mobile, email
                    )
                    if mobile is True:
                        has_mobile += 1
                    if email is True:
                        has_email += 1
                elif resp.status_code == 404:
                    not_found += 1
                    log.info("  Not found in PDL")
                else:
                    errors += 1
                    log.warning(
                        "  Error %s: %s",
                        resp.status_code, resp.text[:200]
                    )

            except Exception as e:
                errors += 1
                log.error("  Request failed: %s", e)

        log.info("=" * 50)
        log.info("PDL COVERAGE PREVIEW RESULTS")
        log.info("  Checked          : %s", total)
        log.info("  Profile found    : %s", found)
        log.info("  Not in PDL       : %s", not_found)
        log.info("  Errors           : %s", errors)
        log.info("  Has mobile phone : %s / %s", has_mobile, found)
        log.info("  Has personal email: %s / %s", has_email, found)
        log.info("  Est. phones if paid: ~%s", has_mobile)
        log.info("=" * 50)

    finally:
        db.close()


def backfill_leads_table() -> None:
    """Backfill A/B/C physicians to leads table with completeness flags."""
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT npi, mobile_phone, phone_dnc_clear, email, practice_email
                FROM physician
                WHERE lead_tier IN ('A', 'B', 'C')
                """
            )
        ).mappings().all()

        log.info("Backfilling %s physicians to leads table", len(rows))

        for row in rows:
            npi = str(row["npi"])
            completeness = compute_contact_completeness(
                has_mobile=bool(row.get("mobile_phone") and row.get("phone_dnc_clear")),
                has_personal_email=bool(row.get("email")),
                has_practice_email=bool(row.get("practice_email")),
            )

            db.execute(
                text(
                    """
                    UPDATE physician
                    SET contact_completeness = :completeness,
                        updated_at = :now
                    WHERE npi = :npi
                    """
                ),
                {
                    "npi": npi,
                    "completeness": completeness,
                    "now": datetime.now(timezone.utc),
                },
            )
            sync_to_leads(npi=npi, db=db)

        db.commit()
        log.info("Backfill complete")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Physician phone enrichment via PDL"
    )
    parser.add_argument("--npi", help="Enrich single NPI")
    parser.add_argument("--limit", type=int, help="Batch size")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill all A/B/C leads to leads table",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview PDL phone coverage before purchasing"
    )
    args = parser.parse_args()

    if args.backfill:
        backfill_leads_table()
    elif args.preview:
        preview_phone_coverage()
    else:
        enrich_phones(npi_filter=args.npi, limit=args.limit)
