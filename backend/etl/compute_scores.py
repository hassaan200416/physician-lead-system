# etl/compute_scores.py
# Lead quality scoring engine.
# Calculates a 0-100 composite score across 4 pillars.
# Email is the primary reachability signal (Pillar 1).
# Phone pipeline is not implemented — may be added in future.
#
# Scoring model:
#   Pillar 1 — Reachability:       40 pts (email-driven)
#   Pillar 2 — Practice Structure: 25 pts
#   Pillar 3 — Activity/Validity:  20 pts
#   Pillar 4 — Target Fit:         15 pts
#   Total:                        100 pts
#
# Tiers: A (80-100), B (60-79), C (40-59), Archive (<40)

from typing import Optional, TypedDict
from datetime import datetime, timezone


class LeadScoreResult(TypedDict):
    """Return type for compute_lead_score."""
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
    # Pillar 1 — Reachability (email-driven)
    has_email: bool = False,
    email_confidence: str = "",
) -> LeadScoreResult:
    """
    Computes composite lead quality score across 4 pillars (0-100).

    Email is the dominant signal — a HIGH confidence email alone
    adds 40 points (Pillar 1 maximum), pushing most physicians
    from Tier C into Tier A.

    Args:
        practice_size:        Number of physicians at the same address.
                              None means unknown — partial credit given.
        solo_practice:        True if physician is the only one at location.
        large_system:         True if organization is a hospital or large network.
                              Large systems score 0 on Pillar 2 (hard to reach).
        is_active_npi:        True if NPI is not deactivated in NPPES.
        has_valid_license:    True if at least one license passed format validation.
        nppes_updated_recently: True if NPPES record was updated within 730 days.
        has_valid_grad_year:  True if graduation year is real (not inferred from
                              enumeration date).
        specialty_match:      True if specialty is in target campaign bucket.
        experience_match:     True if experience bucket matches target range.
        geo_match:            True if state is in target geography.
        multi_state:          True if physician holds licenses in multiple states.
        has_email:            True if a verified email address exists.
        email_confidence:     Confidence level of email: "HIGH", "MEDIUM", or "".
                              HIGH = Hunter score ≥70 + passed all free pre-filters.
                              MEDIUM = Hunter score 40-69 + passed pre-filters.

    Returns:
        LeadScoreResult with:
            lead_score_current:         Total score 0-100 (rounded to 2 decimals)
            lead_tier:                  "A", "B", "C", or "Archive"
            pillar_reachability:        Pillar 1 score (0-40)
            pillar_practice_structure:  Pillar 2 score (0-25)
            pillar_activity_validity:   Pillar 3 score (0-20)
            pillar_target_fit:          Pillar 4 score (0-15)
            scored_at:                  ISO timestamp of when score was computed
    """

    # ── PILLAR 1: REACHABILITY (max 40) ───────────────────
    # Email is the primary reachability signal.
    # Without an email, this pillar contributes 0 points.
    # HIGH confidence email = full 40pts (dominant signal).
    # MEDIUM confidence = 20pts (half credit, email uncertain).
    p1 = 0.0

    if has_email:
        if email_confidence == "HIGH":
            p1 = 40   # Full Pillar 1 — verified, high confidence
        elif email_confidence == "MEDIUM":
            p1 = 20   # Half credit — found but lower confidence

    p1 = min(40, p1)

    # ── PILLAR 2: PRACTICE STRUCTURE (max 25) ─────────────
    # Solo practices and small groups are preferred — easier to
    # reach the actual decision maker directly.
    # Hospital systems score 0 because calls route through
    # switchboards and rarely reach the physician.
    p2 = 0.0

    if large_system:
        p2 = 0        # Hospital/large network — not reachable directly
    elif solo_practice:
        p2 = 25       # Solo practice — physician IS the decision maker
    elif practice_size is not None:
        if practice_size <= 5:
            p2 = 20   # Small group — high access
        elif practice_size <= 15:
            p2 = 12   # Mid-size group — moderate access
        else:
            p2 = 5    # Large group — lower direct access
    else:
        p2 = 10       # Unknown size — give partial credit

    # ── PILLAR 3: ACTIVITY & VALIDITY (max 20) ────────────
    # Measures whether the record is fresh and the physician
    # is currently practicing. Stale or inactive records
    # waste outreach effort.
    p3 = 0.0

    if is_active_npi:
        p3 += 8   # NPI is active — physician is still practicing
    if has_valid_license:
        p3 += 5   # Has at least one valid license on file
    if nppes_updated_recently:
        p3 += 4   # NPPES record updated within last 2 years
    if has_valid_grad_year:
        p3 += 3   # Graduation year confirmed (not inferred)

    p3 = min(20, p3)

    # ── PILLAR 4: TARGET FIT (max 15) ─────────────────────
    # Measures how well this physician matches the campaign target.
    # All inputs are currently defaulted to True in base pipeline —
    # this pillar will be refined when campaign targeting is configured.
    p4 = 0.0

    if specialty_match:
        p4 += 7   # Specialty is in target campaign bucket
    if experience_match:
        p4 += 3   # Experience level matches target range
    if geo_match:
        p4 += 3   # State is in target geography
    if multi_state:
        p4 += 2   # Multi-state license = broader reach / larger practice

    p4 = min(15, p4)

    # ── TOTAL & TIER ASSIGNMENT ────────────────────────────
    total = p1 + p2 + p3 + p4
    total = round(min(100.0, total), 2)

    if total >= 80:
        tier = "A"       # High priority — act immediately
    elif total >= 60:
        tier = "B"       # Medium priority
    elif total >= 40:
        tier = "C"       # Low priority — needs enrichment
    else:
        tier = "Archive" # Not actionable currently

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
    Determines whether a lead should be moved to Archive tier.

    A lead is archived if its score drops below 40 (not actionable)
    or if the physician has been inactive for more than 2 years.

    Args:
        score:         Current lead_score_current value.
        days_inactive: Number of days since last NPPES update or
                       physician activity signal.

    Returns:
        True if the lead should be archived, False otherwise.
    """
    if score < 40:
        return True    # Score too low to be worth outreach
    if days_inactive > 730:
        return True    # 2+ years inactive — likely retired or relocated
    return False