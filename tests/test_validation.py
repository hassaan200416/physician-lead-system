# tests/test_validation.py
# Unit tests for all validation functions.
# Run with: pytest tests/

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.validate_records import (
    validate_npi,
    normalize_name,
    normalize_credential,
    build_email_pattern_name,
    validate_zip,
    validate_state,
    validate_license_number,
    validate_graduation_year,
    calculate_experience,
    normalize_gender,
    is_po_box,
    score_address_confidence,
)


# ── NPI TESTS ──────────────────────────────────────────────

def test_valid_npi():
    valid, npi, _reason = validate_npi("1821091745")
    assert valid is True
    assert npi == "1821091745"

def test_invalid_npi_wrong_length():
    valid, _, _ = validate_npi("123456789")
    assert valid is False

def test_invalid_npi_letters():
    valid, _, _ = validate_npi("12345678AB")
    assert valid is False

def test_empty_npi():
    valid, _, _ = validate_npi("")
    assert valid is False

def test_npi_fails_luhn():
    valid, _, reason = validate_npi("1234567890")
    assert valid is False
    assert "Luhn" in reason


# ── NAME TESTS ─────────────────────────────────────────────

def test_normalize_name_basic():
    assert normalize_name("JOHN") == "John"

def test_normalize_name_hyphen():
    assert normalize_name("SMITH-JONES") == "Smith-Jones"

def test_normalize_name_apostrophe():
    assert normalize_name("O'CONNOR") == "O'Connor"

def test_normalize_name_empty():
    assert normalize_name("") == ""

def test_credential_md():
    assert normalize_credential("M.D.") == "MD"

def test_credential_do():
    assert normalize_credential("D.O.") == "DO"

def test_credential_empty():
    assert normalize_credential("") == ""

def test_email_pattern():
    result = build_email_pattern_name("John", "Smith")
    assert result == "john.smith"

def test_email_pattern_hyphen():
    result = build_email_pattern_name("Mary-Jane", "O'Connor")
    assert result == "maryjane.oconnor"

def test_email_pattern_empty():
    result = build_email_pattern_name("", "Smith")
    assert result == ""


# ── ZIP TESTS ──────────────────────────────────────────────

def test_valid_zip():
    valid, zip_code, _ = validate_zip("90210")
    assert valid is True
    assert zip_code == "90210"

def test_zip_with_plus4():
    valid, zip_code, _ = validate_zip("90210-1234")
    assert valid is True
    assert zip_code == "90210"

def test_zip_all_zeros():
    valid, _, _ = validate_zip("00000")
    assert valid is False

def test_zip_empty():
    valid, _, _ = validate_zip("")
    assert valid is False

def test_zip_too_short():
    valid, _, _ = validate_zip("1234")
    assert valid is False


# ── STATE TESTS ────────────────────────────────────────────

def test_valid_state():
    valid, state, _ = validate_state("TX")
    assert valid is True
    assert state == "TX"

def test_state_lowercase():
    valid, state, _ = validate_state("ca")
    assert valid is True
    assert state == "CA"

def test_invalid_state():
    valid, _, _ = validate_state("XX")
    assert valid is False

def test_empty_state():
    valid, _, _ = validate_state("")
    assert valid is False


# ── LICENSE TESTS ──────────────────────────────────────────

def test_valid_license():
    valid, _lic, _status = validate_license_number("MD123456", "PA", {})
    assert valid is True

def test_garbage_license_zeros():
    valid, _, status = validate_license_number("00000000", "TX", {})
    assert valid is False
    assert status == "format_suspicious"

def test_garbage_license_unknown():
    valid, _, _status = validate_license_number("UNKNOWN", "TX", {})
    assert valid is False

def test_license_too_short():
    valid, _, _ = validate_license_number("AB", "TX", {})
    assert valid is False

def test_license_empty():
    valid, _, _ = validate_license_number("", "TX", {})
    assert valid is False


# ── EXPERIENCE TESTS ───────────────────────────────────────

def test_valid_graduation_year():
    valid, year, flag = validate_graduation_year("1995")
    assert valid is True
    assert year == 1995
    assert flag == "actual"

def test_graduation_year_garbage():
    valid, _, flag = validate_graduation_year("0000")
    assert valid is False
    assert flag == "unknown"

def test_graduation_year_too_old():
    valid, _year, flag = validate_graduation_year("1950")
    assert valid is True
    assert flag == "suspicious"

def test_graduation_year_too_recent():
    valid, _year, flag = validate_graduation_year("2025")
    assert valid is True
    assert flag == "suspicious"

def test_experience_bucket_early():
    _years, bucket, _source = calculate_experience(2022, None, "actual")
    assert bucket == "Early Career"

def test_experience_bucket_senior():
    _years, bucket, _source = calculate_experience(1985, None, "actual")
    assert bucket == "Senior"

def test_experience_fallback_enumeration():
    _years, _bucket, source = calculate_experience(None, 2010, "unknown")
    assert source == "estimated_from_enumeration"


# ── GENDER TESTS ───────────────────────────────────────────

def test_gender_male():
    norm, _src, conf = normalize_gender("M")
    assert norm == "male"
    assert conf == "high"

def test_gender_female():
    norm, _src, _conf = normalize_gender("F")
    assert norm == "female"

def test_gender_unknown():
    norm, _src, conf = normalize_gender("")
    assert norm == "unknown"
    assert conf == "low"

def test_gender_no_inference():
    # Must never infer from name — blank stays unknown
    norm, _, _ = normalize_gender("")
    assert norm == "unknown"


# ── ADDRESS TESTS ──────────────────────────────────────────

def test_po_box_detection():
    assert is_po_box("PO BOX 123") is True
    assert is_po_box("P.O. BOX 456") is True
    assert is_po_box("123 Main St") is False

def test_address_confidence_full():
    score = score_address_confidence("123 Main St", True, True, False)
    assert score == 70.0

def test_address_confidence_po_box_penalty():
    score = score_address_confidence("PO BOX 123", True, True, True)
    assert score == 55.0

def test_address_confidence_no_address():
    score = score_address_confidence("", False, False, False)
    assert score == 0.0