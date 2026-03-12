# tests/test_etl.py
# Integration-style tests for ETL scoring logic.

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.compute_scores import compute_lead_score, should_archive


# ── SCORING TESTS ──────────────────────────────────────────

def test_score_solo_active_physician():
    result = compute_lead_score(
        practice_size=1,
        solo_practice=True,
        large_system=False,
        is_active_npi=True,
        has_valid_license=True,
        nppes_updated_recently=True,
        has_valid_grad_year=True,
        specialty_match=True,
        experience_match=True,
        geo_match=True,
        multi_state=False,
    )
    assert result["lead_score_current"] == 58.0
    assert result["lead_tier"] == "C"
    assert result["pillar_practice_structure"] == 25.0
    assert result["pillar_activity_validity"] == 20.0

def test_score_with_phone_becomes_tier_b():
    result = compute_lead_score(
        practice_size=1,
        solo_practice=True,
        large_system=False,
        is_active_npi=True,
        has_valid_license=True,
        nppes_updated_recently=True,
        has_valid_grad_year=True,
        specialty_match=True,
        experience_match=True,
        geo_match=True,
        multi_state=False,
        has_phone=True,
        phone_verified=True,
    )
    assert result["lead_score_current"] == 93.0
    assert result["lead_tier"] == "A"

def test_score_large_system_zero_practice_points():
    result = compute_lead_score(
        practice_size=100,
        solo_practice=False,
        large_system=True,
        is_active_npi=True,
        has_valid_license=True,
        nppes_updated_recently=True,
        has_valid_grad_year=True,
        specialty_match=True,
        experience_match=True,
        geo_match=True,
        multi_state=False,
    )
    assert result["pillar_practice_structure"] == 0.0

def test_score_inactive_physician():
    result = compute_lead_score(
        practice_size=1,
        solo_practice=True,
        large_system=False,
        is_active_npi=False,
        has_valid_license=False,
        nppes_updated_recently=False,
        has_valid_grad_year=False,
        specialty_match=False,
        experience_match=False,
        geo_match=False,
        multi_state=False,
    )
    assert result["pillar_activity_validity"] == 0.0

def test_score_decay():
    result = compute_lead_score(
        practice_size=1,
        solo_practice=True,
        large_system=False,
        is_active_npi=True,
        has_valid_license=True,
        nppes_updated_recently=True,
        has_valid_grad_year=True,
        specialty_match=True,
        experience_match=True,
        geo_match=True,
        multi_state=False,
        has_phone=True,
        phone_verified=True,
        days_since_phone_validated=180,
    )
    # 2 decay periods x 5 = 10 points decay from reachability
    assert result["pillar_reachability"] == 25.0

def test_score_never_exceeds_100():
    result = compute_lead_score(
        practice_size=1,
        solo_practice=True,
        large_system=False,
        is_active_npi=True,
        has_valid_license=True,
        nppes_updated_recently=True,
        has_valid_grad_year=True,
        specialty_match=True,
        experience_match=True,
        geo_match=True,
        multi_state=True,
        has_phone=True,
        phone_verified=True,
        has_email=True,
        email_confidence=0.95,
    )
    assert result["lead_score_current"] <= 100.0

def test_tier_assignment():
    a_result = compute_lead_score(
        practice_size=1, solo_practice=True, large_system=False,
        is_active_npi=True, has_valid_license=True,
        nppes_updated_recently=True, has_valid_grad_year=True,
        specialty_match=True, experience_match=True,
        geo_match=True, multi_state=True,
        has_phone=True, phone_verified=True,
        has_email=True, email_confidence=0.95,
    )
    assert a_result["lead_tier"] == "A"

def test_should_archive_low_score():
    assert should_archive(35.0, 100) is True

def test_should_archive_inactive():
    assert should_archive(75.0, 800) is True

def test_should_not_archive_good_lead():
    assert should_archive(65.0, 100) is False
