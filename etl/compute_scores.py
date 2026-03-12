# etl/compute_scores.py
# Lead quality scoring engine.
# Calculates a 0-100 score across 4 pillars.
# Phone and email reachability (Pillar 1) starts at 0
# and will be filled by experimental pipelines later.

from typing import Optional, TypedDict
from datetime import datetime, timezone


class LeadScoreResult(TypedDict):
    lead_score_current: float
    lead_tier: str
    pillar_reachability: float
    pillar_practice_structure: float
    pillar_activity_validity: float
    pillar_target_fit: float
    scored_at: str


def compute_lead_score(
    # Pillar 2 — Practice Structure
    practice_size: Optional[int],
    solo_practice: bool,
    large_system: bool,
    # Pillar 3 — Activity & Validity
    is_active_npi: bool,
    has_valid_license: bool,
    nppes_updated_recently: bool,
    has_valid_grad_year: bool,
    # Pillar 4 — Target Fit
    specialty_match: bool,
    experience_match: bool,
    geo_match: bool,
    multi_state: bool,
    # Pillar 1 — Reachability (filled later)
    has_phone: bool = False,
    phone_verified: bool = False,
    has_email: bool = False,
    email_confidence: float = 0.0,
    # Score decay
    days_since_phone_validated: Optional[int] = None,
) -> LeadScoreResult:
    """
    Computes lead quality score across 4 pillars.

    Pillar 1 - Reachability:     40 pts (0 at base pipeline)
    Pillar 2 - Practice Structure: 25 pts
    Pillar 3 - Activity & Validity: 20 pts
    Pillar 4 - Target Fit:         15 pts

    Returns dict with total score, tier, and pillar breakdown.
    """

    # ── PILLAR 1: REACHABILITY (max 40) ───────────────────
    p1 = 0.0

    if phone_verified:
        p1 += 35
    elif has_phone:
        p1 += 20

    if has_email and email_confidence >= 0.70:
        p1 += 5

    # Score decay — -5 per 90 days without phone revalidation
    if days_since_phone_validated is not None and has_phone:
        decay_periods = days_since_phone_validated // 90
        decay = min(decay_periods * 5, 20)
        p1 = max(0, p1 - decay)

    p1 = min(40, p1)

    # ── PILLAR 2: PRACTICE STRUCTURE (max 25) ─────────────
    p2 = 0.0

    if large_system:
        p2 = 0
    elif solo_practice:
        p2 = 25
    elif practice_size is not None:
        if practice_size <= 5:
            p2 = 20
        elif practice_size <= 15:
            p2 = 12
        else:
            p2 = 5
    else:
        p2 = 10  # unknown size — give partial credit

    # ── PILLAR 3: ACTIVITY & VALIDITY (max 20) ────────────
    p3 = 0.0

    if is_active_npi:
        p3 += 8
    if has_valid_license:
        p3 += 5
    if nppes_updated_recently:
        p3 += 4
    if has_valid_grad_year:
        p3 += 3

    p3 = min(20, p3)

    # ── PILLAR 4: TARGET FIT (max 15) ─────────────────────
    p4 = 0.0

    if specialty_match:
        p4 += 7
    if experience_match:
        p4 += 3
    if geo_match:
        p4 += 3
    if multi_state:
        p4 += 2

    p4 = min(15, p4)

    # ── TOTAL ──────────────────────────────────────────────
    total = p1 + p2 + p3 + p4
    total = round(min(100.0, total), 2)

    # ── TIER ASSIGNMENT ────────────────────────────────────
    if total >= 80:
        tier = "A"
    elif total >= 60:
        tier = "B"
    elif total >= 40:
        tier = "C"
    else:
        tier = "Archive"

    return {
        "lead_score_current": total,
        "lead_tier": tier,
        "pillar_reachability": round(p1, 2),
        "pillar_practice_structure": round(p2, 2),
        "pillar_activity_validity": round(p3, 2),
        "pillar_target_fit": round(p4, 2),
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }


def should_archive(score: float, days_inactive: int) -> bool:
    """
    Returns True if a lead should be moved to Archive tier.
    Triggers when score < 40 OR physician inactive > 730 days.
    """
    if score < 40:
        return True
    if days_inactive > 730:
        return True
    return False