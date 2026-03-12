# models/physician.py
# The central physician table — one row per physician.
# Every other table links back to this via NPI.

from sqlalchemy import (
    Column, String, Boolean, Date, DateTime,
    SmallInteger, Numeric
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class Physician(Base):
    __tablename__ = "physician"

    # ── IDENTITY ──────────────────────────────────────────
    npi = Column(String(10), primary_key=True, nullable=False)
    entity_type = Column(SmallInteger, default=1, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    npi_deactivation_date = Column(Date, nullable=True)
    npi_enumeration_date = Column(Date, nullable=True)

    # ── NAME RAW (exact from NPPES) ────────────────────────
    first_name_raw = Column(String(100), nullable=True)
    middle_name_raw = Column(String(100), nullable=True)
    last_name_raw = Column(String(100), nullable=True)
    credential_raw = Column(String(50), nullable=True)

    # ── NAME NORMALIZED ────────────────────────────────────
    first_name_clean = Column(String(100), nullable=True)
    last_name_clean = Column(String(100), nullable=True)
    full_name_display = Column(String(250), nullable=True)
    email_pattern_name = Column(String(200), nullable=True)
    credential_normalized = Column(String(10), nullable=True)
    name_last_updated = Column(DateTime(timezone=True), nullable=True)

    # ── SPECIALTY ──────────────────────────────────────────
    primary_taxonomy_code = Column(String(20), nullable=True)
    specialty_name = Column(String(150), nullable=True)
    derived_specialty_category = Column(String(100), nullable=True)
    specialty_inferred = Column(Boolean, default=False)
    specialty_confidence = Column(Numeric(3, 2), nullable=True)

    # ── EXPERIENCE ─────────────────────────────────────────
    graduation_year = Column(SmallInteger, nullable=True)
    years_of_experience = Column(SmallInteger, nullable=True)
    experience_bucket = Column(String(20), nullable=True)
    experience_source = Column(String(30), nullable=True)
    experience_quality_flag = Column(String(20), nullable=True)
    graduation_year_last_seen = Column(DateTime(timezone=True), nullable=True)

    # ── GENDER ─────────────────────────────────────────────
    gender_raw = Column(String(1), nullable=True)
    gender_normalized = Column(String(10), nullable=True)
    gender_source = Column(String(20), default="nppes")
    gender_confidence = Column(String(10), nullable=True)
    gender_last_seen = Column(DateTime(timezone=True), nullable=True)

    # ── DERIVED PROFILE FLAGS ──────────────────────────────
    retirement_risk_flag = Column(Boolean, default=False)
    growth_profile_flag = Column(Boolean, default=False)
    expansion_profile_flag = Column(Boolean, default=False)
    multi_state_flag = Column(Boolean, default=False)
    license_count = Column(SmallInteger, default=0)

    # ── CALLING FEEDBACK (empty at launch) ─────────────────
    last_call_outcome = Column(String(30), nullable=True)
    call_attempt_count = Column(SmallInteger, default=0)
    last_called_at = Column(DateTime(timezone=True), nullable=True)
    score_adjustment_from_calls = Column(Numeric(5, 2), default=0)

    # ── LEAD SCORING ───────────────────────────────────────
    lead_score_current = Column(Numeric(5, 2), nullable=True)
    lead_score_last_updated = Column(DateTime(timezone=True), nullable=True)
    score_decay_applied = Column(Boolean, default=False)
    lead_tier = Column(String(10), nullable=True)

    # ── AUDIT ──────────────────────────────────────────────
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    last_nppes_sync = Column(DateTime(timezone=True), nullable=True)
    data_version = Column(SmallInteger, default=1)

    # ── RELATIONSHIPS ──────────────────────────────────────
    practice_locations = relationship(
        "PracticeLocation",
        back_populates="physician",
        cascade="all, delete-orphan"
    )
    licenses = relationship(
        "License",
        back_populates="physician",
        cascade="all, delete-orphan"
    )
    organization_links = relationship(
        "PhysicianOrganizationLink",
        back_populates="physician",
        cascade="all, delete-orphan"
    )
    field_history = relationship(
        "FieldValueHistory",
        back_populates="physician",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Physician npi={self.npi} name={self.full_name_display}>"