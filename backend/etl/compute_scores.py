# etl/compute_scores.py
#
# Lead scoring and contact categorisation logic.
#
# Two separate concepts live here — do not conflate them:
#
#   lead_tier (score-based)
#   -----------------------
#   Derived from the four-pillar numeric score (0-100).
#   Determines the quality / priority ordering of a lead.
#     A       >= 80 points
#     B       >= 60 points
#     C       >= 40 points
#     Archive  < 40 points
#
#   contact_category (contact-based)
#   ---------------------------------
#   Derived purely from which contact signals are present.
#   Determines whether a lead enters the leads table at all.
#     A        = mobile_phone present AND any email present
#     B        = any email present, no mobile_phone
#     EXCLUDED = no contact info — row is NEVER inserted into leads
#
#   Contact info is the gate. A physician scoring 100 with zero
#   contact info is EXCLUDED. Score only determines ordering within
#   category A and B.
#
# Four scoring pillars
# --------------------
#   Pillar 1 — Reachability    (0-40 pts)  contact signals
#   Pillar 2 — Practice Structure (0-25 pts)  org size
#   Pillar 3 — Activity/Validity  (0-20 pts)  NPI status, license, recency
#   Pillar 4 — Target Fit         (0-15 pts)  specialty, experience, geography
#
# Enrichment source tagging
# -------------------------
# compute_contact_category() is the single source of truth for
# determining a lead's contact_category. Call it after every
# enrichment run that updates contact fields.

from typing import Optional, TypedDict


# ---------------------------------------------------------------------------
# State risk classification
# ---------------------------------------------------------------------------

# CRITICAL states require explicit consent before dialling a mobile number.
# HIGH states require enhanced disclosure.
# All others default to MEDIUM.
# Source: TCPA state law review + Facebook v. Duguid (2021) framework.

STATE_RISK_PROFILES: dict[str, str] = {
    # CRITICAL — consent required before mobile dial
    "CA": "CRITICAL",
    "FL": "CRITICAL",
    # HIGH — enhanced disclosure required
    "WA": "HIGH",
    "VA": "HIGH",
    "CO": "HIGH",
    "CT": "HIGH",
    "NY": "HIGH",
    "IL": "HIGH",
    "PA": "HIGH",
    "OH": "HIGH",
    "NJ": "HIGH",
    "TX": "HIGH",
}


def get_state_risk(state: str) -> str:
    """
    Returns the TCPA risk classification for a given US state.

    Args:
        state: Two-letter state code (case-insensitive).

    Returns:
        'CRITICAL' | 'HIGH' | 'MEDIUM'
    """
    if not state:
        return "MEDIUM"
    return STATE_RISK_PROFILES.get(state.upper(), "MEDIUM")


# ---------------------------------------------------------------------------
# Contact category — the lead table gate
# ---------------------------------------------------------------------------

def compute_contact_category(
    mobile_phone: Optional[str],
    personal_email: Optional[str],
    practice_email: Optional[str],
    legacy_email: Optional[str] = None,
) -> str:
    """
    Determines the contact-based lead category.

    This is the gate that controls whether a physician enters the
    leads table. Score is irrelevant — contact info is the only input.

    Any email signal counts — personal_email (enrichment tools),
    practice_email (NPPES), or the legacy email column (Hunter v1).
    We check all three so no existing data is silently ignored.

    Rules:
        A        = mobile_phone present AND at least one email present
        B        = at least one email present, no mobile_phone
        EXCLUDED = no contact info at all

    Args:
        mobile_phone:   Personal mobile number (any truthy value = present).
        personal_email: Personal email from enrichment tools.
        practice_email: Email from NPPES.
        legacy_email:   Legacy 'email' column value (Hunter v1). Optional.

    Returns:
        'A' | 'B' | 'EXCLUDED'
    """
    has_phone = bool(mobile_phone and mobile_phone.strip())
    has_email = bool(
        (personal_email and personal_email.strip())
        or (practice_email and practice_email.strip())
        or (legacy_email and legacy_email.strip())
    )

    if has_phone and has_email:
        return "A"
    if has_email:
        return "B"
    return "EXCLUDED"


def should_sync_to_leads(
    mobile_phone: Optional[str],
    personal_email: Optional[str],
    practice_email: Optional[str],
    legacy_email: Optional[str] = None,
) -> bool:
    """
    Returns True if this physician should have a row in the leads table.

    A physician must have at least one email signal to enter leads.
    No contact info = excluded entirely regardless of score.

    Args:
        mobile_phone:   Personal mobile number.
        personal_email: Personal email from enrichment.
        practice_email: Email from NPPES.
        legacy_email:   Legacy email column value (Hunter v1).

    Returns:
        True if physician should be in leads table, False otherwise.
    """
    category = compute_contact_category(
        mobile_phone, personal_email, practice_email, legacy_email
    )
    return category != "EXCLUDED"


# ---------------------------------------------------------------------------
# Legacy contact completeness label (kept for backward compat)
# ---------------------------------------------------------------------------

def compute_contact_completeness(
    has_mobile: bool,
    has_personal_email: bool,
    has_practice_email: bool,
) -> str:
    """
    Legacy EXCELLENT/GOOD/PARTIAL/UNCONTACTABLE label.

    Kept for backward compatibility with existing fields.
    New code should use compute_contact_category() instead.

    Returns:
        'EXCELLENT' | 'GOOD' | 'PARTIAL' | 'UNCONTACTABLE'
    """
    count = sum([has_mobile, has_personal_email, has_practice_email])
    if count == 3:
        return "EXCELLENT"
    if count == 2:
        return "GOOD"
    if count == 1:
        return "PARTIAL"
    return "UNCONTACTABLE"


# ---------------------------------------------------------------------------
# Pillar 1 — Reachability score (0-40 pts)
# ---------------------------------------------------------------------------

def compute_reachability_score(
    mobile_phone: Optional[str],
    phone_confidence: Optional[str],
    phone_dnc_clear: Optional[bool],
    personal_email: Optional[str],
    personal_email_confidence: Optional[str],
    practice_email: Optional[str],
    legacy_email: Optional[str] = None,
    legacy_email_confidence: Optional[str] = None,
) -> int:
    """
    Scores contact reachability across three signals (max 40 pts).

    Signal breakdown:
        Personal mobile (max 20 pts):
            HIGH confidence + DNC clear  → 20 pts
            MEDIUM confidence + DNC clear → 12 pts
        Personal email (max 15 pts):
            HIGH confidence              → 15 pts
            MEDIUM confidence            → 8 pts
        Practice email (5 pts):
            Present                      → 5 pts

    For email, checks personal_email first. Falls back to legacy
    email column so existing Hunter v1 records score correctly.

    Args:
        mobile_phone:              Personal mobile number.
        phone_confidence:          HIGH | MEDIUM | LOW.
        phone_dnc_clear:           True = passed DNC check.
        personal_email:            Personal email from enrichment.
        personal_email_confidence: Confidence for personal_email.
        practice_email:            Email from NPPES.
        legacy_email:              Legacy 'email' column (Hunter v1).
        legacy_email_confidence:   Confidence for legacy email.

    Returns:
        Integer score 0-40.
    """
    score = 0
    phone_conf = (phone_confidence or "").upper()

    # Mobile phone signal
    if mobile_phone and phone_dnc_clear:
        if phone_conf == "HIGH":
            score += 20
        elif phone_conf == "MEDIUM":
            score += 12

    # Personal email signal — prefer personal_email, fall back to legacy
    active_email = personal_email or legacy_email
    active_conf  = (personal_email_confidence or legacy_email_confidence or "").upper()

    if active_email:
        if active_conf == "HIGH":
            score += 15
        elif active_conf == "MEDIUM":
            score += 8
        else:
            # Email present but confidence unknown — award minimum points
            score += 5

    # Practice email signal
    if practice_email and practice_email.strip():
        score += 5

    return min(score, 40)


# ---------------------------------------------------------------------------
# Full four-pillar lead score
# ---------------------------------------------------------------------------

class ScoreResult(TypedDict):
    total_score:         int
    lead_tier:           str
    contact_category:    str
    reachability:        int
    practice_structure:  int
    activity_validity:   int
    target_fit:          int


def compute_lead_score(physician: dict) -> ScoreResult:
    """
    Computes the full four-pillar lead score for a physician dict.

    Pillar weights:
        Pillar 1 — Reachability       0-40 pts
        Pillar 2 — Practice Structure 0-25 pts
        Pillar 3 — Activity/Validity  0-20 pts
        Pillar 4 — Target Fit         0-15 pts
        Total                         0-100 pts

    Score tiers (for ordering within contact categories):
        A       >= 80
        B       >= 60
        C       >= 40
        Archive  < 40

    The 'physician' dict must contain the keys listed below.
    Missing keys default to None / falsy — no KeyError is raised.

    Args:
        physician: Dict of physician fields. Expected keys:
            mobile_phone, phone_confidence, phone_dnc_clear,
            personal_email, personal_email_confidence,
            practice_email, email (legacy), email_confidence_level (legacy),
            org_size, npi_status, license_valid, last_update_recent,
            graduation_year, is_target_specialty, experience_bucket,
            state, target_states (list), multi_state_license.

    Returns:
        ScoreResult TypedDict.
    """
    # ── Pillar 1 — Reachability ───────────────────────────────────────────────
    p1 = compute_reachability_score(
        mobile_phone=physician.get("mobile_phone"),
        phone_confidence=physician.get("phone_confidence"),
        phone_dnc_clear=physician.get("phone_dnc_clear"),
        personal_email=physician.get("personal_email"),
        personal_email_confidence=physician.get("personal_email_confidence"),
        practice_email=physician.get("practice_email"),
        legacy_email=physician.get("email"),
        legacy_email_confidence=physician.get("email_confidence_level"),
    )

    # ── Pillar 2 — Practice Structure ─────────────────────────────────────────
    org_size = physician.get("org_size", 1)
    if org_size == 1:
        p2 = 25
    elif org_size <= 5:
        p2 = 20
    elif org_size <= 15:
        p2 = 12
    elif org_size <= 50:
        p2 = 5
    else:
        p2 = 0

    # ── Pillar 3 — Activity / Validity ────────────────────────────────────────
    p3 = 0
    if physician.get("npi_status") == "A":
        p3 += 8
    if physician.get("license_valid"):
        p3 += 5
    if physician.get("last_update_recent"):
        p3 += 4
    if physician.get("graduation_year"):
        p3 += 3

    # ── Pillar 4 — Target Fit ─────────────────────────────────────────────────
    p4 = 0
    if physician.get("is_target_specialty"):
        p4 += 7
    if physician.get("experience_bucket") in ("5-10", "10-20"):
        p4 += 3
    if physician.get("state") in physician.get("target_states", []):
        p4 += 3
    if physician.get("multi_state_license"):
        p4 += 2

    total = p1 + p2 + p3 + p4

    # ── Score-based tier ──────────────────────────────────────────────────────
    if total >= 80:
        lead_tier = "A"
    elif total >= 60:
        lead_tier = "B"
    elif total >= 40:
        lead_tier = "C"
    else:
        lead_tier = "Archive"

    # ── Contact category ──────────────────────────────────────────────────────
    contact_category = compute_contact_category(
        mobile_phone=physician.get("mobile_phone"),
        personal_email=physician.get("personal_email"),
        practice_email=physician.get("practice_email"),
        legacy_email=physician.get("email"),
    )

    return ScoreResult(
        total_score=total,
        lead_tier=lead_tier,
        contact_category=contact_category,
        reachability=p1,
        practice_structure=p2,
        activity_validity=p3,
        target_fit=p4,
    )


def should_archive(score: float, days_inactive: int) -> bool:
    """
    Backward-compatible archive check used by existing tests and scripts.

    Args:
        score:         Current lead score.
        days_inactive: Days since last NPPES update.

    Returns:
        True if physician should be archived.
    """
    if score < 40:
        return True
    if days_inactive > 730:
        return True
    return False