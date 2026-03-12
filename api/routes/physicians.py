# api/routes/physicians.py
# Physician query endpoints.
# All read-only endpoints for querying physician records.

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Any, Optional

from database import get_db
from api.schemas.physician_schema import (
    PhysicianResponse,
    PhysicianListResponse,
    DatabaseStatsResponse,
    PracticeLocationSchema,
    LicenseSchema,
)

router = APIRouter()


@router.get("/", response_model=PhysicianListResponse)
def list_physicians(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Results per page"),
    state: Optional[str] = Query(None, description="Filter by state code"),
    specialty_category: Optional[str] = Query(None, description="Filter by specialty category"),
    tier: Optional[str] = Query(None, description="Filter by lead tier: A, B, C, Archive"),
    experience_bucket: Optional[str] = Query(None, description="Filter by experience bucket"),
    is_active: Optional[bool] = Query(True, description="Filter by active status"),
    min_score: Optional[float] = Query(None, description="Minimum lead score"),
    db: Session = Depends(get_db)
) -> PhysicianListResponse:
    """
    Returns a paginated list of physicians with optional filters.
    Default returns active physicians ordered by lead score descending.
    """
    filters: list[str] = []
    params: dict[str, Any] = {}

    if is_active is not None:
        filters.append("p.is_active = :is_active")
        params["is_active"] = is_active

    if state:
        filters.append("""
            EXISTS (
                SELECT 1 FROM physician_practice_locations pl
                WHERE pl.npi = p.npi AND pl.state = :state
            )
        """)
        params["state"] = state.upper()

    if specialty_category:
        filters.append("p.derived_specialty_category = :specialty_category")
        params["specialty_category"] = specialty_category

    if tier:
        filters.append("p.lead_tier = :tier")
        params["tier"] = tier.upper()

    if experience_bucket:
        filters.append("p.experience_bucket = :experience_bucket")
        params["experience_bucket"] = experience_bucket

    if min_score is not None:
        filters.append("p.lead_score_current >= :min_score")
        params["min_score"] = min_score

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    # Count total
    count_sql = f"SELECT COUNT(*) FROM physician p {where_clause}"
    total = db.execute(text(count_sql), params).scalar()

    # Fetch page
    offset = (page - 1) * page_size
    params["limit"] = page_size
    params["offset"] = offset

    query_sql = f"""
        SELECT
            p.npi, p.is_active, p.full_name_display,
            p.first_name_clean, p.last_name_clean,
            p.credential_normalized, p.email_pattern_name,
            p.primary_taxonomy_code, p.specialty_name,
            p.derived_specialty_category, p.specialty_confidence,
            p.graduation_year, p.years_of_experience,
            p.experience_bucket, p.experience_quality_flag,
            p.gender_normalized,
            p.retirement_risk_flag, p.growth_profile_flag,
            p.expansion_profile_flag, p.multi_state_flag,
            p.license_count,
            p.lead_score_current, p.lead_tier,
            p.created_at, p.updated_at, p.last_nppes_sync
        FROM physician p
        {where_clause}
        ORDER BY p.lead_score_current DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """

    rows = db.execute(text(query_sql), params).fetchall()

    results: list[PhysicianResponse] = []
    for row in rows:
        # Fetch practice locations
        locations = db.execute(text("""
            SELECT address_line_1, address_line_2, city, state, zip,
                   is_primary_location, address_confidence_score, usps_validated
            FROM physician_practice_locations
            WHERE npi = :npi
        """), {"npi": row[0]}).fetchall()

        # Fetch licenses
        licenses = db.execute(text("""
            SELECT license_number, license_state,
                   is_primary_license, verification_status
            FROM license WHERE npi = :npi
        """), {"npi": row[0]}).fetchall()

        results.append(PhysicianResponse(
            npi=row[0],
            is_active=row[1],
            full_name_display=row[2],
            first_name_clean=row[3],
            last_name_clean=row[4],
            credential_normalized=row[5],
            email_pattern_name=row[6],
            primary_taxonomy_code=row[7],
            specialty_name=row[8],
            derived_specialty_category=row[9],
            specialty_confidence=float(row[10]) if row[10] else None,
            graduation_year=row[11],
            years_of_experience=row[12],
            experience_bucket=row[13],
            experience_quality_flag=row[14],
            gender_normalized=row[15],
            retirement_risk_flag=row[16],
            growth_profile_flag=row[17],
            expansion_profile_flag=row[18],
            multi_state_flag=row[19],
            license_count=row[20],
            lead_score_current=float(row[21]) if row[21] else None,
            lead_tier=row[22],
            created_at=row[23],
            updated_at=row[24],
            last_nppes_sync=row[25],
            practice_locations=[
                PracticeLocationSchema(
                    address_line_1=l[0],
                    address_line_2=l[1],
                    city=l[2],
                    state=l[3],
                    zip=l[4],
                    is_primary_location=l[5],
                    address_confidence_score=float(l[6]) if l[6] else None,
                    usps_validated=l[7],
                )
                for l in locations
            ],
            licenses=[
                LicenseSchema(
                    license_number=l[0],
                    license_state=l[1],
                    is_primary_license=l[2],
                    verification_status=l[3],
                )
                for l in licenses
            ],
        ))

    return PhysicianListResponse(
        total=int(total or 0),
        page=page,
        page_size=page_size,
        results=results,
    )


@router.get("/stats", response_model=DatabaseStatsResponse)
def get_database_stats(db: Session = Depends(get_db)) -> DatabaseStatsResponse:
    """
    Returns aggregate statistics about the physician database.
    Useful for dashboard and reporting.
    """
    stats = db.execute(text("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE is_active = TRUE) as active,
            COUNT(*) FILTER (WHERE lead_tier = 'A') as tier_a,
            COUNT(*) FILTER (WHERE lead_tier = 'B') as tier_b,
            COUNT(*) FILTER (WHERE lead_tier = 'C') as tier_c,
            COUNT(*) FILTER (WHERE lead_tier = 'Archive') as archive
        FROM physician
    """)).fetchone()

    org_count = db.execute(
        text("SELECT COUNT(*) FROM organization_master")
    ).scalar()

    last_sync = db.execute(text("""
        SELECT MAX(sync_completed_at) FROM sync_log
        WHERE status = 'completed'
    """)).scalar()

    specialty_rows = db.execute(text("""
        SELECT derived_specialty_category, COUNT(*)
        FROM physician
        WHERE derived_specialty_category IS NOT NULL
        GROUP BY derived_specialty_category
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)).fetchall()

    state_rows = db.execute(text("""
        SELECT pl.state, COUNT(DISTINCT pl.npi)
        FROM physician_practice_locations pl
        JOIN physician p ON pl.npi = p.npi
        WHERE pl.state IS NOT NULL
        GROUP BY pl.state
        ORDER BY COUNT(DISTINCT pl.npi) DESC
        LIMIT 15
    """)).fetchall()

    if stats is None:
        raise HTTPException(status_code=500, detail="Unable to load database statistics")

    return DatabaseStatsResponse(
        total_physicians=int(stats[0] or 0),
        active_physicians=int(stats[1] or 0),
        tier_a_count=int(stats[2] or 0),
        tier_b_count=int(stats[3] or 0),
        tier_c_count=int(stats[4] or 0),
        archive_count=int(stats[5] or 0),
        total_organizations=int(org_count or 0),
        specialty_breakdown={str(r[0]): int(r[1]) for r in specialty_rows if r[0] is not None},
        state_breakdown={str(r[0]): int(r[1]) for r in state_rows if r[0] is not None},
        last_sync=last_sync,
    )


@router.get("/{npi}", response_model=PhysicianResponse)
def get_physician(npi: str, db: Session = Depends(get_db)) -> PhysicianResponse:
    """
    Returns full profile for a single physician by NPI.
    """
    row = db.execute(text("""
        SELECT
            p.npi, p.is_active, p.full_name_display,
            p.first_name_clean, p.last_name_clean,
            p.credential_normalized, p.email_pattern_name,
            p.primary_taxonomy_code, p.specialty_name,
            p.derived_specialty_category, p.specialty_confidence,
            p.graduation_year, p.years_of_experience,
            p.experience_bucket, p.experience_quality_flag,
            p.gender_normalized,
            p.retirement_risk_flag, p.growth_profile_flag,
            p.expansion_profile_flag, p.multi_state_flag,
            p.license_count,
            p.lead_score_current, p.lead_tier,
            p.created_at, p.updated_at, p.last_nppes_sync
        FROM physician p
        WHERE p.npi = :npi
    """), {"npi": npi}).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Physician NPI {npi} not found")

    locations = db.execute(text("""
        SELECT address_line_1, address_line_2, city, state, zip,
               is_primary_location, address_confidence_score, usps_validated
        FROM physician_practice_locations WHERE npi = :npi
    """), {"npi": npi}).fetchall()

    licenses = db.execute(text("""
        SELECT license_number, license_state,
               is_primary_license, verification_status
        FROM license WHERE npi = :npi
    """), {"npi": npi}).fetchall()

    return PhysicianResponse(
        npi=row[0],
        is_active=row[1],
        full_name_display=row[2],
        first_name_clean=row[3],
        last_name_clean=row[4],
        credential_normalized=row[5],
        email_pattern_name=row[6],
        primary_taxonomy_code=row[7],
        specialty_name=row[8],
        derived_specialty_category=row[9],
        specialty_confidence=float(row[10]) if row[10] else None,
        graduation_year=row[11],
        years_of_experience=row[12],
        experience_bucket=row[13],
        experience_quality_flag=row[14],
        gender_normalized=row[15],
        retirement_risk_flag=row[16],
        growth_profile_flag=row[17],
        expansion_profile_flag=row[18],
        multi_state_flag=row[19],
        license_count=row[20],
        lead_score_current=float(row[21]) if row[21] else None,
        lead_tier=row[22],
        created_at=row[23],
        updated_at=row[24],
        last_nppes_sync=row[25],
        practice_locations=[
            PracticeLocationSchema(
                address_line_1=l[0],
                address_line_2=l[1],
                city=l[2],
                state=l[3],
                zip=l[4],
                is_primary_location=l[5],
                address_confidence_score=float(l[6]) if l[6] else None,
                usps_validated=l[7],
            )
            for l in locations
        ],
        licenses=[
            LicenseSchema(
                license_number=l[0],
                license_state=l[1],
                is_primary_license=l[2],
                verification_status=l[3],
            )
            for l in licenses
        ],
    )