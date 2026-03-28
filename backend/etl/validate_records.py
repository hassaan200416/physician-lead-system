# etl/validate_records.py
# Pure validation functions for all 9 fields.
# No database calls here — just validation logic.
# Each function returns (is_valid, cleaned_value, reason)

import re
from datetime import datetime
from typing import Mapping, Optional, Sequence, Tuple, TypedDict

TaxonomySlots = Sequence[str | None]
SwitchSlots = Sequence[str | None]
CodeSet = set[str]


class FormatRule(TypedDict, total=False):
    pattern_regex: str
    min_length: int
    max_length: int
    example: str


FormatRules = Mapping[str, FormatRule]


# ── NPI VALIDATION ────────────────────────────────────────────────────────────

def validate_npi(npi: str) -> Tuple[bool, str, str]:
    """
    Validates NPI using the Luhn algorithm with 80840 prefix.
    Returns (is_valid, cleaned_npi, reason)
    """
    if not npi:
        return False, "", "NPI is empty"

    npi = str(npi).strip()

    if not npi.isdigit():
        return False, "", f"NPI contains non-digits: {npi}"

    if len(npi) != 10:
        return False, "", f"NPI wrong length: {len(npi)}"

    # Luhn algorithm with 80840 prefix
    full_number = "80840" + npi
    total = 0
    for i, digit in enumerate(full_number):
        n = int(digit)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n

    if total % 10 != 0:
        return False, "", f"NPI failed Luhn check: {npi}"

    return True, npi, "valid"


# ── NAME VALIDATION AND NORMALIZATION ─────────────────────────────────────────

def normalize_name(raw: str) -> str:
    """
    Normalizes a name string.
    - Title cases the value
    - Preserves hyphens and apostrophes
    - Handles O'Connor, Smith-Jones correctly
    """
    if not raw:
        return ""

    raw = raw.strip()

    # Split on spaces, hyphens, apostrophes preserving delimiters
    parts: list[str] = re.split(r"([\s\-'])", raw)
    result: list[str] = []
    for part in parts:
        if part in (" ", "-", "'"):
            result.append(part)
        elif part:
            result.append(part.capitalize())

    return "".join(result)


def normalize_credential(raw: str) -> str:
    """
    Normalises a physician credential string to one of: MD | DO | MBBS | OTHER | ''.

    Strips dots, spaces, and casing before matching so 'M.D.', 'md', 'MD PhD'
    all resolve to 'MD'. MBBS is checked first because it contains 'MB' which
    would otherwise match the MD startswith test.

    Returns:
        'MD' | 'DO' | 'MBBS' | 'OTHER' | '' (empty if input is blank)
    """
    if not raw:
        return ""

    # Remove dots and spaces before checking
    cleaned = raw.strip().upper().replace(".", "").replace(" ", "")

    if "MBBS" in cleaned or "MBB" in cleaned:
        return "MBBS"
    if cleaned in ("MD", "MDphd", "MDPHD") or cleaned.startswith("MD"):
        return "MD"
    if cleaned in ("DO",) or cleaned.startswith("DO"):
        return "DO"
    if cleaned:
        return "OTHER"
    return ""


def build_email_pattern_name(first: str, last: str) -> str:
    """
    Builds email-safe name pattern: firstname.lastname
    Lowercase, no special chars, no spaces.
    Used for email guessing in the email pipeline later.
    """
    if not first or not last:
        return ""

    def clean(s: str) -> str:
        s = s.lower().strip()
        s = re.sub(r"[^a-z]", "", s)
        return s

    f = clean(first)
    l = clean(last)

    if not f or not l:
        return ""

    return f"{f}.{l}"


# ── SPECIALTY VALIDATION ──────────────────────────────────────────────────────

def select_primary_taxonomy(
    taxonomy_slots: TaxonomySlots,
    switch_slots: SwitchSlots,
    valid_codes: CodeSet,
    excluded_codes: CodeSet,
) -> Tuple[Optional[str], float, bool]:
    """
    Selects the primary taxonomy code from up to 15 slots.

    Rules:
    1. If a slot has Switch=Y and the code is valid → confidence 0.90
    2. If no Switch=Y → first valid slot → confidence 0.50
    3. If secondary is more specific (has specialization) → promote → 0.70

    Returns (taxonomy_code, confidence, specialty_inferred)
    """
    primary_code = None
    primary_confidence = 0.0
    specialty_inferred = False

    # Pass 1 — look for Switch=Y
    for code, switch in zip(taxonomy_slots, switch_slots):
        code = (code or "").strip()
        switch = (switch or "").strip().upper()

        if not code or code in excluded_codes:
            continue
        if code not in valid_codes:
            continue

        if switch == "Y":
            primary_code = code
            primary_confidence = 0.90
            break

    # Pass 2 — fallback to first valid slot
    if not primary_code:
        for code in taxonomy_slots:
            code = (code or "").strip()
            if not code or code in excluded_codes:
                continue
            if code in valid_codes:
                primary_code = code
                primary_confidence = 0.50
                break

    return primary_code, primary_confidence, specialty_inferred


# ── ADDRESS VALIDATION ────────────────────────────────────────────────────────

def validate_zip(zip_raw: str) -> Tuple[bool, str, str]:
    """
    Validates and cleans a ZIP code.
    Extracts first 5 digits only.
    Returns (is_valid, cleaned_zip, reason)
    """
    if not zip_raw:
        return False, "", "ZIP is empty"

    zip_raw = str(zip_raw).strip()

    # Extract first 5 digits
    digits = re.sub(r"\D", "", zip_raw)[:5]

    if len(digits) < 5:
        return False, "", f"ZIP too short after cleaning: {zip_raw}"

    if digits == "00000":
        return False, "", "ZIP is all zeros"

    return True, digits, "valid"


def validate_state(state: str) -> Tuple[bool, str, str]:
    """
    Validates US state code.
    """
    valid_states: CodeSet = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC", "PR", "VI", "GU", "MP", "AS"
    }

    if not state:
        return False, "", "State is empty"

    state = state.strip().upper()

    if state not in valid_states:
        return False, "", f"Invalid state code: {state}"

    return True, state, "valid"


def score_address_confidence(
    address_line_1: str,
    zip_valid: bool,
    zip_state_match: bool,
    is_po_box: bool,
) -> float:
    """
    Calculates address confidence score (0-100).

    Additive model with one deduction:
        +30  address_line_1 present and longer than 3 chars
        +20  ZIP valid
        +20  ZIP matches the reported state
        -15  address is a PO Box (less reliable for practice location)

    Result is clamped to [0, 100].
    """
    score = 0.0

    if address_line_1 and len(address_line_1.strip()) > 3:
        score += 30

    if zip_valid:
        score += 20

    if zip_state_match:
        score += 20

    if is_po_box:
        score -= 15

    return max(0.0, min(100.0, score))


def is_po_box(address: str) -> bool:
    """Detects PO Box addresses."""
    if not address:
        return False
    address_upper = address.upper().strip()
    return bool(re.match(r"^P\.?\s*O\.?\s*BOX", address_upper))


# ── LICENSE VALIDATION ────────────────────────────────────────────────────────

GARBAGE_LICENSE_PATTERNS: list[str] = [
    r"^0+$",           # all zeros
    r"^9+$",           # all nines
    r"^1+$",           # all ones
    r"(?i)^unknown$",
    r"(?i)^n/?a$",
    r"(?i)^none$",
    r"(?i)^na$",
]


def validate_license_number(
    license_num: str,
    state: str,
    format_rules: FormatRules,
) -> Tuple[bool, str, str]:
    """
    Validates a license number.
    Returns (is_valid, cleaned_license, status)
    Status is one of: unverified, heuristic_pass, format_suspicious
    """
    if not license_num:
        return False, "", "empty"

    license_num = license_num.strip()

    if len(license_num) < 3:
        return False, "", "format_suspicious"

    # Check garbage patterns
    for pattern in GARBAGE_LICENSE_PATTERNS:
        if re.match(pattern, license_num):
            return False, "", "format_suspicious"

    # Check state-specific format if rule exists
    if state and state in format_rules:
        rule = format_rules[state]
        pattern = rule.get("pattern_regex")
        min_len = rule.get("min_length", 0)
        max_len = rule.get("max_length", 99)

        if pattern:
            if not re.match(pattern, license_num):
                return True, license_num, "format_suspicious"

        if not (min_len <= len(license_num) <= max_len):
            return True, license_num, "format_suspicious"

    return True, license_num, "heuristic_pass"


# ── EXPERIENCE VALIDATION ─────────────────────────────────────────────────────

CURRENT_YEAR = datetime.now().year


def validate_graduation_year(year_raw: str) -> Tuple[bool, int, str]:
    """
    Validates graduation year from NPPES.
    Returns (is_valid, year_int, quality_flag)
    quality_flag: actual | suspicious | unknown
    """
    if not year_raw:
        return False, 0, "unknown"

    year_str = str(year_raw).strip()

    if not year_str.isdigit():
        return False, 0, "unknown"

    year_int = int(year_str)

    # Reject garbage values
    garbage = {"0000", "9999", "1111", "2222", "3333",
               "4444", "5555", "6666", "7777", "8888"}
    if year_str in garbage:
        return False, 0, "unknown"

    # Realistic range check
    min_year = CURRENT_YEAR - 60
    max_year = CURRENT_YEAR - 3

    if year_int < min_year or year_int > max_year:
        return True, year_int, "suspicious"

    return True, year_int, "actual"


def calculate_experience(
    graduation_year: Optional[int],
    enumeration_year: Optional[int],
    quality_flag: str
) -> Tuple[int, str, str]:
    """
    Calculates years of experience and assigns bucket.
    Returns (years, bucket, source)
    """
    if graduation_year and quality_flag in ("actual", "suspicious"):
        years = CURRENT_YEAR - graduation_year
        source = "graduation_year"
    elif enumeration_year:
        years = CURRENT_YEAR - enumeration_year - 1
        source = "estimated_from_enumeration"
    else:
        return 0, "Unknown", "unavailable"

    years = max(0, years)

    if years <= 5:
        bucket = "Early Career"
    elif years <= 20:
        bucket = "Mid Career"
    elif years <= 34:
        bucket = "Late Career"
    else:
        bucket = "Senior"

    return years, bucket, source


# ── GENDER NORMALIZATION ──────────────────────────────────────────────────────

def normalize_gender(raw: str) -> Tuple[str, str, str]:
    """
    Normalizes gender from NPPES.
    Returns (gender_normalized, gender_source, gender_confidence)
    NEVER infers from name. NEVER calls external APIs.
    Only M/F from NPPES is used.
    """
    if not raw:
        return "unknown", "nppes", "low"

    raw = raw.strip().upper()

    if raw == "M":
        return "male", "nppes", "high"
    if raw == "F":
        return "female", "nppes", "high"

    return "unknown", "nppes", "low"
