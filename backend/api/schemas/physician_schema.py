# api/schemas/physician_schema.py
# Pydantic schemas for API request and response validation.
# These define exactly what the API accepts and returns.

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


# ── NESTED SCHEMAS ─────────────────────────────────────────

class PracticeLocationSchema(BaseModel):
    address_line_1: Optional[str] = None
    address_line_2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    is_primary_location: Optional[bool] = None
    address_confidence_score: Optional[float] = None
    usps_validated: Optional[bool] = None

    class Config:
        from_attributes = True


class LicenseSchema(BaseModel):
    license_number: Optional[str] = None
    license_state: Optional[str] = None
    is_primary_license: Optional[bool] = None
    verification_status: Optional[str] = None

    class Config:
        from_attributes = True


class OrganizationSchema(BaseModel):
    organization_id: Optional[str] = None
    organization_name_normalized: Optional[str] = None
    practice_size_estimate: Optional[int] = None
    solo_practice_flag: Optional[bool] = None
    large_system_flag: Optional[bool] = None

    class Config:
        from_attributes = True


# ── MAIN PHYSICIAN RESPONSE ────────────────────────────────

class PhysicianResponse(BaseModel):
    # Identity
    npi: str
    is_active: bool
    full_name_display: Optional[str] = None
    first_name_clean: Optional[str] = None
    last_name_clean: Optional[str] = None
    credential_normalized: Optional[str] = None
    email_pattern_name: Optional[str] = None

    # Specialty
    primary_taxonomy_code: Optional[str] = None
    specialty_name: Optional[str] = None
    derived_specialty_category: Optional[str] = None
    specialty_confidence: Optional[float] = None

    # Experience
    graduation_year: Optional[int] = None
    years_of_experience: Optional[int] = None
    experience_bucket: Optional[str] = None
    experience_quality_flag: Optional[str] = None

    # Gender
    gender_normalized: Optional[str] = None

    # Flags
    retirement_risk_flag: Optional[bool] = None
    growth_profile_flag: Optional[bool] = None
    expansion_profile_flag: Optional[bool] = None
    multi_state_flag: Optional[bool] = None
    license_count: Optional[int] = None

    # Scoring
    lead_score_current: Optional[float] = None
    lead_tier: Optional[str] = None

    # Contact (empty at base pipeline)
    phone_number: Optional[str] = None
    email_address: Optional[str] = None

    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_nppes_sync: Optional[datetime] = None

    # Related data
    practice_locations: List[PracticeLocationSchema] = []
    licenses: List[LicenseSchema] = []

    class Config:
        from_attributes = True


# ── LIST RESPONSE ──────────────────────────────────────────

class PhysicianListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: List[PhysicianResponse]


# ── FILTER REQUEST ─────────────────────────────────────────

class PhysicianFilterParams(BaseModel):
    specialty_category: Optional[str] = Field(
        None,
        description="Filter by campaign bucket: Primary Care, Specialist, Surgical"
    )
    state: Optional[str] = Field(
        None,
        description="Filter by 2-letter state code e.g. TX, CA, NY"
    )
    min_score: Optional[float] = Field(
        None,
        description="Minimum lead score (0-100)"
    )
    tier: Optional[str] = Field(
        None,
        description="Filter by tier: A, B, C, Archive"
    )
    experience_bucket: Optional[str] = Field(
        None,
        description="Early Career, Mid Career, Late Career, Senior"
    )
    is_active: Optional[bool] = Field(
        True,
        description="Filter by active NPI status"
    )
    solo_practice_only: Optional[bool] = Field(
        None,
        description="Return only solo practitioners"
    )
    exclude_large_systems: Optional[bool] = Field(
        None,
        description="Exclude hospital system physicians"
    )


# ── SYNC LOG RESPONSE ──────────────────────────────────────

class SyncLogResponse(BaseModel):
    sync_id: str
    sync_started_at: datetime
    sync_completed_at: Optional[datetime] = None
    source_file: Optional[str] = None
    records_processed: Optional[int] = None
    records_inserted: Optional[int] = None
    records_updated: Optional[int] = None
    records_failed: Optional[int] = None
    error_rate_pct: Optional[float] = None
    status: Optional[str] = None
    notes: Optional[str] = None

    class Config:
        from_attributes = True


# ── STATS RESPONSE ─────────────────────────────────────────

class DatabaseStatsResponse(BaseModel):
    total_physicians: int
    active_physicians: int
    tier_a_count: int
    tier_b_count: int
    tier_c_count: int
    archive_count: int
    total_organizations: int
    specialty_breakdown: dict[str, int]
    state_breakdown: dict[str, int]
    last_sync: Optional[datetime] = None