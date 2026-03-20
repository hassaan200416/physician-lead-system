from typing import Optional, TypedDict


# State risk classification for compliance routing.
STATE_RISK_PROFILES = {
    # CRITICAL - consent required before mobile dial
    "CA": "CRITICAL",
    "FL": "CRITICAL",
    # HIGH - enhanced disclosure required
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
    # All others default to MEDIUM
}


def get_state_risk(state: str) -> str:
    if not state:
        return "MEDIUM"
    return STATE_RISK_PROFILES.get(state.upper(), "MEDIUM")


def compute_contact_completeness(
    has_mobile: bool,
    has_personal_email: bool,
    has_practice_email: bool,
) -> str:
    """Derive contact completeness label from available contact signals."""
    count = sum([has_mobile, has_personal_email, has_practice_email])

    if count == 3:
        return "EXCELLENT"
    if count == 2:
        return "GOOD"
    if count == 1:
        return "PARTIAL"
    return "UNCONTACTABLE"


def compute_reachability_score(
    mobile_phone: Optional[str],
    phone_confidence: Optional[str],
    phone_dnc_clear: Optional[bool],
    personal_email: Optional[str],
    email_confidence: Optional[str],
    practice_email: Optional[str],
) -> int:
    """
    Scores contact reachability across three signals.

    Personal mobile: up to 20 pts
    Personal email: up to 15 pts
    Practice email: up to 5 pts
    """
    score = 0
    phone_conf = (phone_confidence or "").upper()
    email_conf = (email_confidence or "").upper()

    if mobile_phone and phone_dnc_clear:
        if phone_conf == "HIGH":
            score += 20
        elif phone_conf == "MEDIUM":
            score += 12

    if personal_email:
        if email_conf == "HIGH":
            score += 15
        elif email_conf == "MEDIUM":
            score += 8

    if practice_email:
        score += 5

    return min(score, 40)


class ScoreResult(TypedDict):
    total_score: int
    tier: str
    contact_completeness: str
    reachability: int
    practice_structure: int
    activity_validity: int
    target_fit: int
    is_uncontactable: bool


def compute_lead_score(physician: dict) -> ScoreResult:
    """Compute four-pillar lead score with contact completeness output."""
    p1 = compute_reachability_score(
        mobile_phone=physician.get("mobile_phone"),
        phone_confidence=physician.get("phone_confidence"),
        phone_dnc_clear=physician.get("phone_dnc_clear"),
        personal_email=physician.get("email"),
        email_confidence=physician.get("email_confidence"),
        practice_email=physician.get("practice_email"),
    )

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

    p3 = 0
    if physician.get("npi_status") == "A":
        p3 += 8
    if physician.get("license_valid"):
        p3 += 5
    if physician.get("last_update_recent"):
        p3 += 4
    if physician.get("graduation_year"):
        p3 += 3

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

    if total >= 80:
        tier = "A"
    elif total >= 60:
        tier = "B"
    elif total >= 40:
        tier = "C"
    else:
        tier = "Archive"

    completeness = compute_contact_completeness(
        has_mobile=bool(physician.get("mobile_phone") and physician.get("phone_dnc_clear")),
        has_personal_email=bool(physician.get("email")),
        has_practice_email=bool(physician.get("practice_email")),
    )

    return {
        "total_score": total,
        "tier": tier,
        "contact_completeness": completeness,
        "reachability": p1,
        "practice_structure": p2,
        "activity_validity": p3,
        "target_fit": p4,
        "is_uncontactable": completeness == "UNCONTACTABLE",
    }


def should_sync_to_leads(score_result: ScoreResult) -> bool:
    """Leads sync if tier is actionable; UNCONTACTABLE is still retained."""
    return score_result["tier"] in ("A", "B", "C")


def should_archive(score: float, days_inactive: int) -> bool:
    """Backward-compatible archive check used by existing tests and scripts."""
    if score < 40:
        return True
    if days_inactive > 730:
        return True
    return False