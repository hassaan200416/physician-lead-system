# models/physician.py
#
# Central physician record — one row per individual provider.
# Every other table in the system links back here via NPI.
#
# Column groups (in order):
#   IDENTITY            — NPI, entity type, active status
#   NAME RAW            — exact strings from NPPES source file
#   NAME NORMALIZED     — cleaned / display-ready versions
#   SPECIALTY           — taxonomy code + derived category
#   EXPERIENCE          — graduation year, years, bucket
#   GENDER              — raw + normalized + source
#   CONTACT — PHONE     — mobile enrichment results (enrich_phones.py)
#   CONTACT — EMAIL     — email enrichment results (enrich_emails.py)
#   CONTACT — LEGACY    — original Hunter v1 columns (read-only)
#   ENRICHMENT AUDIT    — sources array, attempt timestamps
#   DERIVED FLAGS       — computed booleans
#   CALLING FEEDBACK    — outcome data from AI calling agent
#   LEAD SCORING        — score, tier, last updated
#   AUDIT               — created_at, updated_at, sync timestamps
#
# Email column convention
# -----------------------
#   personal_email      canonical write target for all enrichment tools
#   practice_email      sourced from NPPES directly (less reliable)
#   email               LEGACY — Hunter v1 data, read-only going forward
#                       Kept to avoid data loss. New code reads personal_email.
#
# Phone column convention
# -----------------------
#   mobile_phone        personal mobile found by enrichment tools
#   All phone columns populated by enrich_phones.py only.
#
# contact_category (on leads table, not here) is the authoritative
# contact-based tier:
#   A = mobile_phone + any email
#   B = any email, no mobile_phone
#   Rows with no contact info are never inserted into leads.
#
# Migration history
# -----------------
#   340e952d67cc                  — base table created
#   002_phone_enrichment          — phone columns + contact_completeness
#                                   added directly to DB (migration was a pass)
#   003_enrichment_sources_and_stats — personal_email, enrichment_sources,
#                                      email_enriched_at,
#                                      enrichment_last_attempted_at added

from sqlalchemy import (
    Column, String, Boolean, Date, DateTime,
    Integer, SmallInteger, Numeric, Text
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class Physician(Base):
    __tablename__ = "physician"

    # ── IDENTITY ──────────────────────────────────────────────────────────────
    npi                   = Column(String(10), primary_key=True, nullable=False)
    entity_type           = Column(SmallInteger, default=1, nullable=False)
    is_active             = Column(Boolean, default=True, nullable=False)
    npi_deactivation_date = Column(Date, nullable=True)
    npi_enumeration_date  = Column(Date, nullable=True)

    # ── NAME RAW (exact strings from NPPES) ───────────────────────────────────
    first_name_raw  = Column(String(100), nullable=True)
    middle_name_raw = Column(String(100), nullable=True)
    last_name_raw   = Column(String(100), nullable=True)
    credential_raw  = Column(String(50),  nullable=True)

    # ── NAME NORMALIZED ───────────────────────────────────────────────────────
    first_name_clean    = Column(String(100), nullable=True)
    last_name_clean     = Column(String(100), nullable=True)
    full_name_display   = Column(String(250), nullable=True)
    email_pattern_name  = Column(String(200), nullable=True)
    credential_normalized = Column(String(10), nullable=True)
    name_last_updated   = Column(DateTime(timezone=True), nullable=True)

    # ── SPECIALTY ─────────────────────────────────────────────────────────────
    primary_taxonomy_code    = Column(String(20),  nullable=True)
    specialty_name           = Column(String(150), nullable=True)
    derived_specialty_category = Column(String(100), nullable=True)
    specialty_inferred       = Column(Boolean, default=False)
    specialty_confidence     = Column(Numeric(3, 2), nullable=True)

    # ── EXPERIENCE ────────────────────────────────────────────────────────────
    graduation_year          = Column(SmallInteger, nullable=True)
    years_of_experience      = Column(SmallInteger, nullable=True)
    experience_bucket        = Column(String(20), nullable=True)
    experience_source        = Column(String(30), nullable=True)
    experience_quality_flag  = Column(String(20), nullable=True)
    graduation_year_last_seen = Column(DateTime(timezone=True), nullable=True)

    # ── GENDER ────────────────────────────────────────────────────────────────
    gender_raw        = Column(String(1),  nullable=True)
    gender_normalized = Column(String(10), nullable=True)
    gender_source     = Column(String(20), default="nppes")
    gender_confidence = Column(String(10), nullable=True)
    gender_last_seen  = Column(DateTime(timezone=True), nullable=True)

    # ── CONTACT — PHONE ───────────────────────────────────────────────────────
    # Populated exclusively by enrich_phones.py.
    # phone_enrichment_attempted is the deduplication guard —
    # set True on first attempt whether or not data was found.

    mobile_phone               = Column(String(20),  nullable=True)
    phone_confidence           = Column(String(10),  nullable=True,
                                        doc="HIGH | MEDIUM | LOW")
    phone_line_type            = Column(String(20),  nullable=True,
                                        doc="mobile | landline | voip — via Twilio")
    phone_carrier              = Column(String(100), nullable=True)
    phone_dnc_checked          = Column(Boolean, default=False, nullable=True)
    phone_dnc_clear            = Column(Boolean, nullable=True,
                                        doc="True = not on DNC. None = not yet checked.")
    phone_state_risk           = Column(String(10),  nullable=True,
                                        doc="CRITICAL | HIGH | MEDIUM")
    phone_enrichment_attempted = Column(Boolean, default=False, nullable=False)
    phone_enriched_at          = Column(DateTime(timezone=True), nullable=True)

    # ── CONTACT — EMAIL (canonical) ───────────────────────────────────────────
    # personal_email  = enrichment tools (Hunter v2+, Apollo, ContactOut, etc.)
    # practice_email  = sourced from NPPES
    # email_enrichment_attempted = deduplication guard (already existed in DB)

    personal_email            = Column(String(255), nullable=True,
                                       doc="Canonical personal email. "
                                           "All new enrichment sources write here.")
    personal_email_confidence = Column(String(10),  nullable=True,
                                       doc="HIGH | MEDIUM | LOW")
    practice_email            = Column(String(255), nullable=True,
                                       doc="Email from NPPES. Less reliable for outreach.")
    email_enrichment_attempted = Column(Boolean, default=False, nullable=False,
                                        doc="Deduplication guard. Set True on first attempt.")
    email_enriched_at         = Column(DateTime(timezone=True), nullable=True)

    # ── CONTACT — EMAIL LEGACY (Hunter v1, read-only) ─────────────────────────
    # These columns were written by the original enrich_emails.py.
    # Do NOT write new data here. Read personal_email instead.
    # Kept to avoid data loss on the 18 existing Hunter records.

    email                    = Column(String(255), nullable=True,
                                      doc="LEGACY Hunter v1 email. Read-only. "
                                          "Use personal_email for all new work.")
    email_confidence_score   = Column(Integer,     nullable=True,
                                      doc="LEGACY Hunter score 0-100.")
    email_confidence_level   = Column(String(10),  nullable=True,
                                      doc="LEGACY HIGH | MEDIUM | LOW.")
    email_verification_status = Column(String(50), nullable=True,
                                       doc="LEGACY verification status string.")
    email_source             = Column(String(50),  nullable=True,
                                      doc="LEGACY source tag e.g. 'hunter_io'.")
    email_acquired_at        = Column(DateTime(timezone=True), nullable=True,
                                      doc="LEGACY acquisition timestamp.")
    email_enrichment_result  = Column(String(100), nullable=True,
                                      doc="LEGACY result string e.g. 'found_pass1'.")

    # ── CONTACT — COMPLETENESS ────────────────────────────────────────────────
    # Legacy label. The authoritative contact gate is contact_category
    # on the leads table. This field is no longer recomputed.

    contact_completeness = Column(String(20), nullable=True,
                                  doc="LEGACY label. See leads.contact_category.")

    # ── ENRICHMENT AUDIT ──────────────────────────────────────────────────────
    # enrichment_sources is append-only. Values must match source_name
    # in enrichment_source_stats exactly.

    enrichment_sources = Column(
        ARRAY(Text),
        default=list,
        server_default="'{}'::text[]",
        nullable=False,
        doc="Append-only list of tool names that contributed contact data. "
            "Example: [\"hunter.io\", \"apollo\"]. "
            "Values must match enrichment_source_stats.source_name."
    )
    enrichment_last_attempted_at = Column(
        DateTime(timezone=True), nullable=True,
        doc="Last time any enrichment tool attempted this physician. "
            "Schedulers skip records processed within the last N days."
    )

    # ── DERIVED PROFILE FLAGS ─────────────────────────────────────────────────
    retirement_risk_flag  = Column(Boolean, default=False)
    growth_profile_flag   = Column(Boolean, default=False)
    expansion_profile_flag = Column(Boolean, default=False)
    multi_state_flag      = Column(Boolean, default=False)
    license_count         = Column(SmallInteger, default=0)

    # ── CALLING FEEDBACK ──────────────────────────────────────────────────────
    last_call_outcome           = Column(String(30), nullable=True)
    call_attempt_count          = Column(SmallInteger, default=0)
    last_called_at              = Column(DateTime(timezone=True), nullable=True)
    score_adjustment_from_calls = Column(Numeric(5, 2), default=0)

    # ── LEAD SCORING ──────────────────────────────────────────────────────────
    lead_score_current      = Column(Numeric(5, 2), nullable=True)
    lead_score_last_updated = Column(DateTime(timezone=True), nullable=True)
    score_decay_applied     = Column(Boolean, default=False)
    lead_tier               = Column(String(10), nullable=True)

    # ── AUDIT ─────────────────────────────────────────────────────────────────
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    last_nppes_sync = Column(DateTime(timezone=True), nullable=True)
    data_version    = Column(SmallInteger, default=1)

    # ── RELATIONSHIPS ─────────────────────────────────────────────────────────
    practice_locations = relationship(
        "PracticeLocation",
        back_populates="physician",
        cascade="all, delete-orphan",
    )
    licenses = relationship(
        "License",
        back_populates="physician",
        cascade="all, delete-orphan",
    )
    organization_links = relationship(
        "PhysicianOrganizationLink",
        back_populates="physician",
        cascade="all, delete-orphan",
    )
    field_history = relationship(
        "FieldValueHistory",
        back_populates="physician",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Physician npi={self.npi} name={self.full_name_display}>"