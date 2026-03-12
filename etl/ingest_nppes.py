# etl/ingest_nppes.py
# Main ETL pipeline for NPPES data.
# Processes the NPPES CSV file in chunks and upserts
# all physician records into the database.
#
# Usage:
#   python etl/ingest_nppes.py           (auto-finds NPPES file)
#   python etl/ingest_nppes.py --limit 1000  (process first 1000 rows only)
#   python etl/ingest_nppes.py --file path/to/file.csv

import sys
import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, cast

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import engine
from etl.validate_records import (
    validate_npi, normalize_name, normalize_credential,
    build_email_pattern_name, select_primary_taxonomy,
    validate_zip, validate_state, score_address_confidence,
    is_po_box, validate_license_number, validate_graduation_year,
    calculate_experience, normalize_gender, FormatRule
)
from etl.compute_scores import compute_lead_score
from etl.download_nppes import find_nppes_csv
from dotenv import load_dotenv

load_dotenv()

# ── NPPES COLUMN MAPPINGS ──────────────────────────────────────────────────────
# These are the exact column names in the NPPES CSV file

NPI_COL = "NPI"
ENTITY_TYPE_COL = "Entity Type Code"
DEACTIVATION_DATE_COL = "NPI Deactivation Date"
ENUMERATION_DATE_COL = "NPI Enumeration Date"
LAST_UPDATE_COL = "Last Update Date"

# Name columns
FIRST_NAME_COL = "Provider First Name"
MIDDLE_NAME_COL = "Provider Middle Name"
LAST_NAME_COL = "Provider Last Name (Legal Name)"
CREDENTIAL_COL = "Provider Credential Text"

# Practice address columns (NOT mailing)
ADDR1_COL = "Provider First Line Business Practice Location Address"
ADDR2_COL = "Provider Second Line Business Practice Location Address"
CITY_COL = "Provider Business Practice Location Address City Name"
STATE_COL = "Provider Business Practice Location Address State Name"
ZIP_COL = "Provider Business Practice Location Address Postal Code"

# Taxonomy columns (15 slots)
TAXONOMY_COLS = [f"Healthcare Provider Taxonomy Code_{i}" for i in range(1, 16)]
SWITCH_COLS = [
    f"Healthcare Provider Primary Taxonomy Switch_{i}" for i in range(1, 16)
]

# License columns (15 slots)
LICENSE_COLS = [f"Provider License Number_{i}" for i in range(1, 16)]
LICENSE_STATE_COLS = [
    f"Provider License Number State Code_{i}" for i in range(1, 16)
]
LICENSE_TAXONOMY_COLS = [
    f"Healthcare Provider Taxonomy Code_{i}" for i in range(1, 16)
]

# Gender column
GENDER_COL = "Provider Gender Code"

# Graduation year column
GRAD_YEAR_COL = "Provider Graduation Year"

# All columns we need to read (reduces memory usage)
REQUIRED_COLUMNS = (
    [NPI_COL, ENTITY_TYPE_COL, DEACTIVATION_DATE_COL,
     ENUMERATION_DATE_COL, LAST_UPDATE_COL,
     FIRST_NAME_COL, MIDDLE_NAME_COL, LAST_NAME_COL, CREDENTIAL_COL,
     ADDR1_COL, ADDR2_COL, CITY_COL, STATE_COL, ZIP_COL,
     GENDER_COL, GRAD_YEAR_COL]
    + TAXONOMY_COLS + SWITCH_COLS
    + LICENSE_COLS + LICENSE_STATE_COLS
)

CHUNK_SIZE = 50_000

ReferenceData = dict[str, Any]
ProcessedRow = dict[str, Any]
AddressRecord = dict[str, Any]
LicenseRecord = dict[str, Any]
PhysicianAddressPair = tuple[str, AddressRecord]
PhysicianLicensesPair = tuple[str, list[LicenseRecord]]


# ── REFERENCE DATA LOADER ──────────────────────────────────────────────────────

def load_reference_data(conn: Connection) -> ReferenceData:
    """Loads all reference tables into memory for fast lookup."""
    print("Loading reference data into memory...")

    # Valid physician taxonomy codes
    result = conn.execute(text(
        "SELECT taxonomy_code, classification, specialization, campaign_bucket "
        "FROM nucc_taxonomy_reference WHERE is_physician = TRUE"
    ))
    taxonomy_map: dict[str, dict[str, str | None]] = {}
    for row in result:
        taxonomy_map[row[0]] = {
            "classification": row[1],
            "specialization": row[2],
            "campaign_bucket": row[3],
        }

    # Excluded taxonomy codes
    result = conn.execute(text(
        "SELECT taxonomy_code FROM excluded_taxonomy_codes"
    ))
    excluded_codes: set[str] = {str(row[0]) for row in result}

    # ZIP to state mapping
    result = conn.execute(text(
        "SELECT zip_code, state_code FROM zip_state_reference"
    ))
    zip_state_map: dict[str, str] = {
        str(row[0]): str(row[1]) for row in result
    }

    # License format rules
    result = conn.execute(text(
        "SELECT state_code, pattern_regex, min_length, max_length "
        "FROM license_format_rules"
    ))
    license_rules: dict[str, FormatRule] = {}
    for row in result:
        license_rules[row[0]] = {
            "pattern_regex": row[1],
            "min_length": row[2],
            "max_length": row[3],
        }

    print(f"  Loaded {len(taxonomy_map)} physician taxonomy codes")
    print(f"  Loaded {len(excluded_codes)} excluded codes")
    print(f"  Loaded {len(zip_state_map)} ZIP codes")
    print(f"  Loaded {len(license_rules)} license format rules")

    return {
        "taxonomy_map": taxonomy_map,
        "excluded_codes": excluded_codes,
        "valid_codes": set(taxonomy_map.keys()),
        "zip_state_map": zip_state_map,
        "license_rules": license_rules,
    }


# ── ROW PROCESSOR ─────────────────────────────────────────────────────────────

def process_row(row: pd.Series, ref: ReferenceData) -> Optional[ProcessedRow]:
    """
    Processes a single NPPES row.
    Returns a dict of cleaned physician data or None if row should be skipped.
    """

    # ── STEP 1: ENTITY TYPE FILTER ────────────────────────
    entity_type = str(row.get(ENTITY_TYPE_COL, "")).strip()
    if entity_type != "1":
        return None  # Only individual physicians (Type 1)

    # ── STEP 2: NPI VALIDATION ────────────────────────────
    npi_raw = str(row.get(NPI_COL, "")).strip()
    npi_valid, npi, _npi_reason = validate_npi(npi_raw)
    if not npi_valid:
        return None  # Hard discard — invalid NPI

    # ── STEP 3: ACTIVE STATUS ─────────────────────────────
    deact_date = str(row.get(DEACTIVATION_DATE_COL, "")).strip()
    is_active = not bool(deact_date and deact_date not in ("", "nan", "NaT"))

    # ── STEP 4: TAXONOMY FILTER ───────────────────────────
    taxonomy_slots: list[str | None] = [
        (str(row.get(col, "")).strip() or None) for col in TAXONOMY_COLS
    ]
    switch_slots: list[str | None] = [
        (str(row.get(col, "")).strip() or None) for col in SWITCH_COLS
    ]

    valid_codes = cast(set[str], ref["valid_codes"])
    excluded_codes = cast(set[str], ref["excluded_codes"])

    primary_code, specialty_confidence, specialty_inferred = select_primary_taxonomy(
        taxonomy_slots, switch_slots,
        valid_codes, excluded_codes
    )

    if not primary_code:
        return None  # Not a physician specialty we target

    # Get specialty details from taxonomy map
    taxonomy_map = cast(dict[str, dict[str, str | None]], ref["taxonomy_map"])
    tax_info = taxonomy_map.get(primary_code, {})
    specialty_name = tax_info.get("classification", "")
    specialization = tax_info.get("specialization", "")
    if specialization:
        specialty_name = f"{specialty_name} - {specialization}"
    derived_category = tax_info.get("campaign_bucket", "")

    # ── STEP 5: NAME NORMALIZATION ────────────────────────
    first_raw = str(row.get(FIRST_NAME_COL, "")).strip()
    middle_raw = str(row.get(MIDDLE_NAME_COL, "")).strip()
    last_raw = str(row.get(LAST_NAME_COL, "")).strip()
    credential_raw = str(row.get(CREDENTIAL_COL, "")).strip()

    # Drop if both first and last name are null
    if (not first_raw or first_raw == "nan") and \
       (not last_raw or last_raw == "nan"):
        return None

    first_clean = normalize_name(first_raw) if first_raw != "nan" else ""
    last_clean = normalize_name(last_raw) if last_raw != "nan" else ""
    middle_clean = normalize_name(middle_raw) if middle_raw not in ("", "nan") else ""
    credential_normalized = normalize_credential(credential_raw)

    full_name_parts = [first_clean]
    if middle_clean:
        full_name_parts.append(middle_clean)
    full_name_parts.append(last_clean)
    if credential_normalized:
        full_name_parts.append(credential_normalized)
    full_name_display = " ".join(p for p in full_name_parts if p)

    email_pattern = build_email_pattern_name(first_clean, last_clean)

    # ── STEP 6: ADDRESS ───────────────────────────────────
    addr1_raw = str(row.get(ADDR1_COL, "")).strip()
    addr2_raw = str(row.get(ADDR2_COL, "")).strip()
    city_raw = str(row.get(CITY_COL, "")).strip()
    state_raw = str(row.get(STATE_COL, "")).strip()
    zip_raw = str(row.get(ZIP_COL, "")).strip()

    addr1 = addr1_raw if addr1_raw not in ("", "nan") else None
    addr2 = addr2_raw if addr2_raw not in ("", "nan") else None
    city = city_raw if city_raw not in ("", "nan") else None

    state_valid, state_clean, _ = validate_state(state_raw)
    state = state_clean if state_valid else None

    zip_valid, zip_clean, _ = validate_zip(zip_raw)
    zip_code = zip_clean if zip_valid else None

    # ZIP-state cross validation
    zip_state_match = False
    zip_state_map = cast(dict[str, str], ref["zip_state_map"])
    if zip_code and state:
        expected_state = zip_state_map.get(zip_code)
        zip_state_match = (expected_state == state)

    po_box = is_po_box(addr1 or "")
    addr_confidence = score_address_confidence(
        addr1 or "", zip_valid, zip_state_match, po_box
    )

    # ── STEP 7: EXPERIENCE ────────────────────────────────
    grad_year_raw = str(row.get(GRAD_YEAR_COL, "")).strip()
    enum_date_raw = str(row.get(ENUMERATION_DATE_COL, "")).strip()

    grad_valid, grad_year, exp_quality = validate_graduation_year(grad_year_raw)

    enum_year = None
    if enum_date_raw and enum_date_raw not in ("", "nan"):
        try:
            enum_year = int(str(enum_date_raw)[:4])
        except (ValueError, TypeError):
            pass

    years_exp, exp_bucket, exp_source = calculate_experience(
        grad_year if grad_valid else None,
        enum_year,
        exp_quality
    )

    # Derived flags
    retirement_risk = years_exp >= 35
    growth_profile = (exp_bucket == "Early Career")
    expansion_profile = (exp_bucket == "Mid Career")

    # ── STEP 8: GENDER ────────────────────────────────────
    gender_raw = str(row.get(GENDER_COL, "")).strip()
    gender_norm, gender_src, gender_conf = normalize_gender(gender_raw)

    # ── STEP 9: LICENSES ──────────────────────────────────
    licenses: list[LicenseRecord] = []
    license_rules = cast(dict[str, FormatRule], ref["license_rules"])
    for i, (lic_col, state_col) in enumerate(
        zip(LICENSE_COLS, LICENSE_STATE_COLS)
    ):
        lic_num = str(row.get(lic_col, "")).strip()
        lic_state = str(row.get(state_col, "")).strip()

        if not lic_num or lic_num in ("", "nan"):
            continue
        if not lic_state or lic_state in ("", "nan"):
            continue

        lic_valid, lic_clean, lic_status = validate_license_number(
            lic_num, lic_state, license_rules
        )

        if not lic_valid:
            continue

        # First license slot linked to primary taxonomy is primary
        is_primary = (i == 0)

        licenses.append({
            "license_number": lic_clean,
            "license_state": lic_state.strip().upper(),
            "is_primary_license": is_primary,
            "verification_status": lic_status,
            "format_valid": lic_status == "heuristic_pass",
            "linked_taxonomy_code": primary_code if is_primary else None,
        })

    has_valid_license = any(
        l["verification_status"] in ("heuristic_pass",)
        for l in licenses
    )
    license_count = len(licenses)
    multi_state = len({l["license_state"] for l in licenses}) > 1

    # ── STEP 10: LEAD SCORE ───────────────────────────────
    last_update_raw = str(row.get(LAST_UPDATE_COL, "")).strip()
    nppes_updated_recently = False
    if last_update_raw and last_update_raw not in ("", "nan"):
        try:
            update_date = pd.to_datetime(last_update_raw, errors="coerce")
            if pd.notna(update_date):
                days_since = (datetime.now() - update_date.to_pydatetime().replace(tzinfo=None)).days
                nppes_updated_recently = days_since <= 730
        except Exception:
            pass

    score_result = compute_lead_score(
        practice_size=None,
        solo_practice=False,
        large_system=False,
        is_active_npi=is_active,
        has_valid_license=has_valid_license,
        nppes_updated_recently=nppes_updated_recently,
        has_valid_grad_year=grad_valid and exp_quality == "actual",
        specialty_match=True,
        experience_match=True,
        geo_match=True,
        multi_state=multi_state,
    )

    # ── ASSEMBLE RESULT ───────────────────────────────────
    return {
        "npi": npi,
        "is_active": is_active,
        "entity_type": 1,
        "first_name_raw": first_raw if first_raw != "nan" else None,
        "middle_name_raw": middle_raw if middle_raw not in ("", "nan") else None,
        "last_name_raw": last_raw if last_raw != "nan" else None,
        "credential_raw": credential_raw if credential_raw not in ("", "nan") else None,
        "first_name_clean": first_clean or None,
        "last_name_clean": last_clean or None,
        "full_name_display": full_name_display or None,
        "email_pattern_name": email_pattern or None,
        "credential_normalized": credential_normalized or None,
        "primary_taxonomy_code": primary_code,
        "specialty_name": specialty_name or None,
        "derived_specialty_category": derived_category or None,
        "specialty_inferred": specialty_inferred,
        "specialty_confidence": float(specialty_confidence),
        "graduation_year": grad_year if grad_valid else None,
        "years_of_experience": years_exp or None,
        "experience_bucket": exp_bucket,
        "experience_source": exp_source,
        "experience_quality_flag": exp_quality,
        "retirement_risk_flag": retirement_risk,
        "growth_profile_flag": growth_profile,
        "expansion_profile_flag": expansion_profile,
        "multi_state_flag": multi_state,
        "license_count": license_count,
        "gender_raw": gender_raw if gender_raw not in ("", "nan") else None,
        "gender_normalized": gender_norm,
        "gender_source": gender_src,
        "gender_confidence": gender_conf,
        "lead_score_current": score_result["lead_score_current"],
        "lead_tier": score_result["lead_tier"],
        "address": {
            "address_line_1": addr1,
            "address_line_2": addr2,
            "city": city,
            "state": state,
            "zip": zip_code,
            "is_primary_location": True,
            "address_confidence_score": addr_confidence,
        },
        "licenses": licenses,
    }


# ── DATABASE UPSERT ───────────────────────────────────────────────────────────

def upsert_physician(conn: Connection, data: ProcessedRow, now: datetime) -> None:
    """Upserts a single physician record into the database."""

    conn.execute(text("""
        INSERT INTO physician (
            npi, entity_type, is_active,
            first_name_raw, middle_name_raw, last_name_raw, credential_raw,
            first_name_clean, last_name_clean, full_name_display,
            email_pattern_name, credential_normalized, name_last_updated,
            primary_taxonomy_code, specialty_name, derived_specialty_category,
            specialty_inferred, specialty_confidence,
            graduation_year, years_of_experience, experience_bucket,
            experience_source, experience_quality_flag,
            retirement_risk_flag, growth_profile_flag, expansion_profile_flag,
            multi_state_flag, license_count,
            gender_raw, gender_normalized, gender_source, gender_confidence,
            gender_last_seen,
            lead_score_current, lead_tier, lead_score_last_updated,
            created_at, updated_at, last_nppes_sync
        ) VALUES (
            :npi, :entity_type, :is_active,
            :first_name_raw, :middle_name_raw, :last_name_raw, :credential_raw,
            :first_name_clean, :last_name_clean, :full_name_display,
            :email_pattern_name, :credential_normalized, :name_last_updated,
            :primary_taxonomy_code, :specialty_name, :derived_specialty_category,
            :specialty_inferred, :specialty_confidence,
            :graduation_year, :years_of_experience, :experience_bucket,
            :experience_source, :experience_quality_flag,
            :retirement_risk_flag, :growth_profile_flag, :expansion_profile_flag,
            :multi_state_flag, :license_count,
            :gender_raw, :gender_normalized, :gender_source, :gender_confidence,
            :gender_last_seen,
            :lead_score_current, :lead_tier, :lead_now,
            :now, :now, :now
        )
        ON CONFLICT (npi) DO UPDATE SET
            is_active                = EXCLUDED.is_active,
            first_name_raw           = EXCLUDED.first_name_raw,
            last_name_raw            = EXCLUDED.last_name_raw,
            credential_raw           = EXCLUDED.credential_raw,
            first_name_clean         = EXCLUDED.first_name_clean,
            last_name_clean          = EXCLUDED.last_name_clean,
            full_name_display        = EXCLUDED.full_name_display,
            email_pattern_name       = EXCLUDED.email_pattern_name,
            credential_normalized    = EXCLUDED.credential_normalized,
            name_last_updated        = EXCLUDED.name_last_updated,
            primary_taxonomy_code    = EXCLUDED.primary_taxonomy_code,
            specialty_name           = EXCLUDED.specialty_name,
            derived_specialty_category = EXCLUDED.derived_specialty_category,
            specialty_inferred       = EXCLUDED.specialty_inferred,
            specialty_confidence     = EXCLUDED.specialty_confidence,
            graduation_year          = EXCLUDED.graduation_year,
            years_of_experience      = EXCLUDED.years_of_experience,
            experience_bucket        = EXCLUDED.experience_bucket,
            experience_source        = EXCLUDED.experience_source,
            experience_quality_flag  = EXCLUDED.experience_quality_flag,
            retirement_risk_flag     = EXCLUDED.retirement_risk_flag,
            growth_profile_flag      = EXCLUDED.growth_profile_flag,
            expansion_profile_flag   = EXCLUDED.expansion_profile_flag,
            multi_state_flag         = EXCLUDED.multi_state_flag,
            license_count            = EXCLUDED.license_count,
            gender_raw               = EXCLUDED.gender_raw,
            gender_normalized        = EXCLUDED.gender_normalized,
            lead_score_current       = EXCLUDED.lead_score_current,
            lead_tier                = EXCLUDED.lead_tier,
            lead_score_last_updated  = EXCLUDED.lead_score_last_updated,
            updated_at               = EXCLUDED.updated_at,
            last_nppes_sync          = EXCLUDED.last_nppes_sync
    """), {**data, "name_last_updated": now, "gender_last_seen": now,
           "lead_now": now, "now": now})


def upsert_practice_location(
    conn: Connection,
    npi: str,
    addr: AddressRecord,
    now: datetime,
) -> None:
    """Upserts practice location for a physician."""
    if not addr.get("address_line_1"):
        return

    conn.execute(text("""
        INSERT INTO physician_practice_locations (
            location_id, npi, address_line_1, address_line_2,
            city, state, zip,
            is_primary_location, is_mailing_address,
            address_confidence_score, address_last_updated,
            address_last_seen, source_name, created_at
        ) VALUES (
            gen_random_uuid(), :npi, :addr1, :addr2,
            :city, :state, :zip,
            :is_primary, FALSE,
            :confidence, :now,
            :now, 'nppes', :now
        )
        ON CONFLICT DO NOTHING
    """), {
        "npi": npi,
        "addr1": addr.get("address_line_1"),
        "addr2": addr.get("address_line_2"),
        "city": addr.get("city"),
        "state": addr.get("state"),
        "zip": addr.get("zip"),
        "is_primary": addr.get("is_primary_location", True),
        "confidence": addr.get("address_confidence_score", 0),
        "now": now,
    })


def upsert_licenses(
    conn: Connection,
    npi: str,
    licenses: list[LicenseRecord],
    now: datetime,
) -> None:
    """Upserts license records for a physician."""
    for lic in licenses:
        conn.execute(text("""
            INSERT INTO license (
                license_id, npi, license_number, license_state,
                is_primary_license, verification_status, format_valid,
                linked_taxonomy_code, source, last_seen_date, created_at
            ) VALUES (
                gen_random_uuid(), :npi, :number, :state,
                :is_primary, :status, :format_valid,
                :taxonomy_code, 'nppes', :now, :now
            )
            ON CONFLICT DO NOTHING
        """), {
            "npi": npi,
            "number": lic["license_number"],
            "state": lic["license_state"],
            "is_primary": lic["is_primary_license"],
            "status": lic["verification_status"],
            "format_valid": lic["format_valid"],
            "taxonomy_code": lic.get("linked_taxonomy_code"),
            "now": now,
        })


# ── ORGANIZATION CLUSTERING ───────────────────────────────────────────────────

def cluster_organizations(conn: Connection, now: datetime) -> int:
    """
    Discovers organizations by clustering physicians
    who share the same address.
    Groups by address_line_1 + zip and assigns cluster IDs.
    """
    print("Clustering organizations by shared address...")

    # Find addresses with 2+ physicians
    result = conn.execute(text("""
        SELECT
            UPPER(TRIM(address_line_1)) as addr,
            zip,
            state,
            COUNT(DISTINCT npi) as physician_count,
            array_agg(DISTINCT npi) as npis
        FROM physician_practice_locations
        WHERE address_line_1 IS NOT NULL
          AND zip IS NOT NULL
        GROUP BY UPPER(TRIM(address_line_1)), zip, state
        HAVING COUNT(DISTINCT npi) >= 2
    """))

    cluster_count = 0
    org_count = 0

    for row in result:
        addr = row[0]
        zip_code = row[1]
        state = row[2]
        npi_list = row[4]

        # Generate a stable cluster ID
        cluster_seed = f"{addr}|{zip_code}"
        cluster_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, cluster_seed))

        # Update practice_cluster_id on all matching locations
        conn.execute(text("""
            UPDATE physician_practice_locations
            SET practice_cluster_id = :cluster_id
            WHERE UPPER(TRIM(address_line_1)) = :addr
              AND zip = :zip
        """), {"cluster_id": cluster_id, "addr": addr, "zip": zip_code})

        # Create or update organization record
        conn.execute(text("""
            INSERT INTO organization_master (
                organization_id, organization_name_raw,
                organization_name_normalized,
                address_line_1, state, zip,
                practice_size_estimate, source,
                first_seen_date, last_seen_date,
                created_at, updated_at
            ) VALUES (
                :org_id, :name, :name_norm,
                :addr, :state, :zip,
                :size, 'nppes_clustering',
                :now, :now, :now, :now
            )
            ON CONFLICT (organization_id) DO UPDATE SET
                practice_size_estimate = EXCLUDED.practice_size_estimate,
                last_seen_date = EXCLUDED.last_seen_date,
                updated_at = EXCLUDED.updated_at
        """), {
            "org_id": cluster_id,
            "name": addr,
            "name_norm": addr,
            "addr": addr,
            "state": state,
            "zip": zip_code,
            "size": len(npi_list),
            "now": now,
        })

        # Link all physicians to this organization
        for npi in npi_list:
            conn.execute(text("""
                INSERT INTO physician_organization_link (
                    link_id, npi, organization_id,
                    link_type, source, created_at
                ) VALUES (
                    gen_random_uuid(), :npi, :org_id,
                    'practice_member', 'nppes_clustering', :now
                )
                ON CONFLICT DO NOTHING
            """), {"npi": npi, "org_id": cluster_id, "now": now})

        cluster_count += 1
        org_count += 1

    conn.commit()
    print(f"  Created/updated {org_count} organizations from {cluster_count} address clusters")
    return org_count


# ── MAIN ETL RUNNER ───────────────────────────────────────────────────────────

def run_etl(csv_path: str, limit: Optional[int] = None) -> None:
    """
    Main ETL function.
    Reads NPPES CSV in chunks and processes each row.
    """
    sync_start = datetime.now(timezone.utc)
    now = datetime.now(timezone.utc)

    print("=" * 60)
    print("PHYSICIAN LEAD SYSTEM — NPPES ETL")
    print(f"Started: {sync_start.isoformat()}")
    print(f"File: {csv_path}")
    if limit:
        print(f"Limit: {limit} rows")
    print("=" * 60)

    # Load reference data once into memory
    with engine.connect() as conn:
        ref = load_reference_data(conn)

    # Counters
    rows_processed = 0
    rows_inserted = 0
    rows_updated = 0
    rows_failed = 0
    rows_skipped = 0

    # Start sync log entry
    sync_id = str(uuid.uuid4())
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO sync_log (
                sync_id, sync_started_at, source_file, status
            ) VALUES (
                :id, :started, :file, 'running'
            )
        """), {
            "id": sync_id,
            "started": sync_start,
            "file": str(csv_path),
        })
        conn.commit()

    # Determine columns to read
    try:
        sample = pd.read_csv(csv_path, nrows=1)
        available_cols = set(sample.columns.tolist())
        cols_to_read = [c for c in REQUIRED_COLUMNS if c in available_cols]

        # Check for graduation year column
        if GRAD_YEAR_COL not in available_cols:
            print(f"  Note: '{GRAD_YEAR_COL}' column not found — experience will use enumeration year")
            cols_to_read = [c for c in cols_to_read if c != GRAD_YEAR_COL]

    except Exception as e:
        print(f"Error reading CSV headers: {e}")
        return

    print(f"\nReading CSV in chunks of {CHUNK_SIZE:,} rows...")
    print("-" * 60)

    batch_physicians: list[ProcessedRow] = []
    batch_addresses: list[PhysicianAddressPair] = []
    batch_licenses: list[PhysicianLicensesPair] = []
    BATCH_SIZE = 500

    chunk_iter = pd.read_csv(
        csv_path,
        usecols=cols_to_read,
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
    )

    for chunk_num, chunk in enumerate(chunk_iter, 1):
        if limit and rows_processed >= limit:
            break

        chunk_start = datetime.now()

        for _, row in chunk.iterrows():
            if limit and rows_processed >= limit:
                break

            rows_processed += 1

            try:
                result = process_row(row, ref)
                if result is None:
                    rows_skipped += 1
                    continue

                batch_physicians.append(result)
                batch_addresses.append((result["npi"], result["address"]))
                batch_licenses.append((result["npi"], result["licenses"]))

                if len(batch_physicians) >= BATCH_SIZE:
                    inserted, updated, failed = flush_batch(
                        batch_physicians, batch_addresses, batch_licenses, now
                    )
                    rows_inserted += inserted
                    rows_updated += updated
                    rows_failed += failed
                    batch_physicians, batch_addresses, batch_licenses = [], [], []

            except Exception as e:
                rows_failed += 1
                if rows_failed <= 10:  # Only print first 10 errors
                    print(f"  Error processing NPI {row.get(NPI_COL, 'unknown')}: {e}")

        chunk_elapsed = (datetime.now() - chunk_start).seconds
        total_kept = rows_inserted + rows_updated
        print(
            f"  Chunk {chunk_num}: {len(chunk):,} rows in {chunk_elapsed}s | "
            f"Total kept: {total_kept:,} | "
            f"Skipped: {rows_skipped:,} | "
            f"Failed: {rows_failed}"
        )

    # Flush remaining batch
    if batch_physicians:
        inserted, updated, failed = flush_batch(
            batch_physicians, batch_addresses, batch_licenses, now
        )
        rows_inserted += inserted
        rows_updated += updated
        rows_failed += failed

    # Run organization clustering
    print()
    with engine.connect() as conn:
        org_count = cluster_organizations(conn, now)

    # Update lead scores based on org size
    print("Updating lead scores with organization data...")
    update_scores_with_org_data(now)

    # Complete sync log
    sync_end = datetime.now(timezone.utc)
    duration = (sync_end - sync_start).seconds
    error_rate = (rows_failed / rows_processed * 100) if rows_processed > 0 else 0

    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE sync_log SET
                sync_completed_at   = :completed,
                records_processed   = :processed,
                records_inserted    = :inserted,
                records_updated     = :updated,
                records_failed      = :failed,
                error_rate_pct      = :error_rate,
                status              = 'completed',
                notes               = :notes
            WHERE sync_id = :id
        """), {
            "id": sync_id,
            "completed": sync_end,
            "processed": rows_processed,
            "inserted": rows_inserted,
            "updated": rows_updated,
            "failed": rows_failed,
            "error_rate": round(error_rate, 2),
            "notes": f"Duration: {duration}s | Organizations: {org_count}",
        })
        conn.commit()

    print()
    print("=" * 60)
    print("ETL COMPLETE")
    print(f"  Duration:   {duration}s")
    print(f"  Processed:  {rows_processed:,}")
    print(f"  Inserted:   {rows_inserted:,}")
    print(f"  Updated:    {rows_updated:,}")
    print(f"  Skipped:    {rows_skipped:,}")
    print(f"  Failed:     {rows_failed}")
    print(f"  Error rate: {error_rate:.2f}%")
    print("=" * 60)


def flush_batch(
    physicians: list[ProcessedRow],
    addresses: list[PhysicianAddressPair],
    licenses: list[PhysicianLicensesPair],
    now: datetime,
) -> tuple[int, int, int]:
    """
    Writes a batch of physicians using bulk INSERT.
    Much faster than individual inserts — one round trip per batch.
    """
    inserted = 0
    updated = 0
    failed = 0

    if not physicians:
        return inserted, updated, failed

    try:
        with engine.connect() as conn:
            bulk_physician_rows: list[dict[str, Any]] = [{
                **p,
                "name_last_updated": now,
                "gender_last_seen": now,
                "lead_now": now,
                "now": now
            } for p in physicians]

            # Bulk upsert all physicians in one statement
            conn.execute(text("""
                INSERT INTO physician (
                    npi, entity_type, is_active,
                    first_name_raw, middle_name_raw, last_name_raw, credential_raw,
                    first_name_clean, last_name_clean, full_name_display,
                    email_pattern_name, credential_normalized, name_last_updated,
                    primary_taxonomy_code, specialty_name, derived_specialty_category,
                    specialty_inferred, specialty_confidence,
                    graduation_year, years_of_experience, experience_bucket,
                    experience_source, experience_quality_flag,
                    retirement_risk_flag, growth_profile_flag, expansion_profile_flag,
                    multi_state_flag, license_count,
                    gender_raw, gender_normalized, gender_source, gender_confidence,
                    gender_last_seen,
                    lead_score_current, lead_tier, lead_score_last_updated,
                    created_at, updated_at, last_nppes_sync
                ) VALUES (
                    :npi, :entity_type, :is_active,
                    :first_name_raw, :middle_name_raw, :last_name_raw, :credential_raw,
                    :first_name_clean, :last_name_clean, :full_name_display,
                    :email_pattern_name, :credential_normalized, :name_last_updated,
                    :primary_taxonomy_code, :specialty_name, :derived_specialty_category,
                    :specialty_inferred, :specialty_confidence,
                    :graduation_year, :years_of_experience, :experience_bucket,
                    :experience_source, :experience_quality_flag,
                    :retirement_risk_flag, :growth_profile_flag, :expansion_profile_flag,
                    :multi_state_flag, :license_count,
                    :gender_raw, :gender_normalized, :gender_source, :gender_confidence,
                    :gender_last_seen,
                    :lead_score_current, :lead_tier, :lead_now,
                    :now, :now, :now
                )
                ON CONFLICT (npi) DO UPDATE SET
                    is_active                = EXCLUDED.is_active,
                    first_name_raw           = EXCLUDED.first_name_raw,
                    last_name_raw            = EXCLUDED.last_name_raw,
                    credential_raw           = EXCLUDED.credential_raw,
                    first_name_clean         = EXCLUDED.first_name_clean,
                    last_name_clean          = EXCLUDED.last_name_clean,
                    full_name_display        = EXCLUDED.full_name_display,
                    email_pattern_name       = EXCLUDED.email_pattern_name,
                    credential_normalized    = EXCLUDED.credential_normalized,
                    name_last_updated        = EXCLUDED.name_last_updated,
                    primary_taxonomy_code    = EXCLUDED.primary_taxonomy_code,
                    specialty_name           = EXCLUDED.specialty_name,
                    derived_specialty_category = EXCLUDED.derived_specialty_category,
                    specialty_inferred       = EXCLUDED.specialty_inferred,
                    specialty_confidence     = EXCLUDED.specialty_confidence,
                    graduation_year          = EXCLUDED.graduation_year,
                    years_of_experience      = EXCLUDED.years_of_experience,
                    experience_bucket        = EXCLUDED.experience_bucket,
                    experience_source        = EXCLUDED.experience_source,
                    experience_quality_flag  = EXCLUDED.experience_quality_flag,
                    retirement_risk_flag     = EXCLUDED.retirement_risk_flag,
                    growth_profile_flag      = EXCLUDED.growth_profile_flag,
                    expansion_profile_flag   = EXCLUDED.expansion_profile_flag,
                    multi_state_flag         = EXCLUDED.multi_state_flag,
                    license_count            = EXCLUDED.license_count,
                    gender_raw               = EXCLUDED.gender_raw,
                    gender_normalized        = EXCLUDED.gender_normalized,
                    lead_score_current       = EXCLUDED.lead_score_current,
                    lead_tier                = EXCLUDED.lead_tier,
                    lead_score_last_updated  = EXCLUDED.lead_score_last_updated,
                    updated_at               = EXCLUDED.updated_at,
                    last_nppes_sync          = EXCLUDED.last_nppes_sync
            """), bulk_physician_rows)

            conn.commit()
            inserted = len(physicians)

    except Exception as e:
        # If bulk fails, fall back to individual inserts
        print(f"    Bulk insert failed, falling back to individual: {e}")
        for data in physicians:
            try:
                with engine.connect() as conn2:
                    upsert_physician(conn2, data, now)
                    conn2.commit()
                    inserted += 1
            except Exception as e2:
                failed += 1
                if failed <= 3:
                    print(f"    DB error for {data.get('npi')}: {e2}")

    # Bulk insert addresses
    addr_rows: list[PhysicianAddressPair] = [
        (npi, addr) for npi, addr in addresses
        if addr.get("address_line_1")
    ]
    if addr_rows:
        try:
            with engine.connect() as conn:
                addr_params: list[dict[str, Any]] = [{
                    "npi": npi,
                    "addr1": addr.get("address_line_1"),
                    "addr2": addr.get("address_line_2"),
                    "city": addr.get("city"),
                    "state": addr.get("state"),
                    "zip": addr.get("zip"),
                    "is_primary": addr.get("is_primary_location", True),
                    "confidence": addr.get("address_confidence_score", 0),
                    "now": now,
                } for npi, addr in addr_rows]

                conn.execute(text("""
                    INSERT INTO physician_practice_locations (
                        location_id, npi, address_line_1, address_line_2,
                        city, state, zip,
                        is_primary_location, is_mailing_address,
                        address_confidence_score, address_last_updated,
                        address_last_seen, source_name, created_at
                    ) VALUES (
                        gen_random_uuid(), :npi, :addr1, :addr2,
                        :city, :state, :zip,
                        :is_primary, FALSE,
                        :confidence, :now, :now, 'nppes', :now
                    )
                    ON CONFLICT DO NOTHING
                """), addr_params)
                conn.commit()
        except Exception:
            pass

    # Bulk insert licenses
    lic_rows: list[dict[str, Any]] = []
    for npi, lics in licenses:
        for lic in lics:
            lic_rows.append({"npi": npi, **lic, "now": now})

    if lic_rows:
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO license (
                        license_id, npi, license_number, license_state,
                        is_primary_license, verification_status, format_valid,
                        linked_taxonomy_code, source, last_seen_date, created_at
                    ) VALUES (
                        gen_random_uuid(), :npi, :license_number, :license_state,
                        :is_primary_license, :verification_status, :format_valid,
                        :linked_taxonomy_code, 'nppes', :now, :now
                    )
                    ON CONFLICT DO NOTHING
                """), lic_rows)
                conn.commit()
        except Exception:
            pass

    return inserted, updated, failed


def update_scores_with_org_data(now: datetime):
    """
    Updates lead scores for physicians now that we know
    their organization size. This runs after clustering.
    """
    with engine.connect() as conn:
        # Get all physicians with their org size
        result = conn.execute(text("""
            SELECT
                p.npi,
                p.is_active,
                p.multi_state_flag,
                p.experience_quality_flag,
                p.license_count,
                COALESCE(o.practice_size_estimate, 1) as practice_size,
                o.solo_practice_flag,
                o.large_system_flag
            FROM physician p
            LEFT JOIN physician_organization_link pol ON p.npi = pol.npi
            LEFT JOIN organization_master o ON pol.organization_id = o.organization_id
        """))

        updates: list[dict[str, Any]] = []
        for row in result:
            npi = row[0]
            is_active = row[1]
            multi_state = row[2]
            exp_quality = row[3]
            license_count = row[4]
            practice_size = row[5] or 1
            solo = row[6] or (practice_size == 1)
            large_system = row[7] or False

            score_result = compute_lead_score(
                practice_size=practice_size,
                solo_practice=solo,
                large_system=large_system,
                is_active_npi=is_active,
                has_valid_license=(license_count > 0),
                nppes_updated_recently=True,
                has_valid_grad_year=(exp_quality == "actual"),
                specialty_match=True,
                experience_match=True,
                geo_match=True,
                multi_state=multi_state,
            )

            updates.append({
                "npi": npi,
                "score": score_result["lead_score_current"],
                "tier": score_result["lead_tier"],
                "now": now,
            })

        if updates:
            conn.execute(text("""
                UPDATE physician SET
                    lead_score_current = :score,
                    lead_tier = :tier,
                    lead_score_last_updated = :now
                WHERE npi = :npi
            """), updates)
            conn.commit()
            print(f"  Updated scores for {len(updates):,} physicians")


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NPPES ETL Pipeline"
    )
    parser.add_argument(
        "--file",
        help="Path to NPPES CSV file (auto-detected if not specified)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of rows to process (useful for testing)"
    )
    args = parser.parse_args()

    if args.file:
        csv_path = args.file
    else:
        try:
            csv_path = str(find_nppes_csv())
        except FileNotFoundError as e:
            print(e)
            sys.exit(1)

    run_etl(csv_path, limit=args.limit)
