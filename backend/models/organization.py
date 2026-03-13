# models/organization.py
# Stores discovered practice entities and links them to physicians.

from sqlalchemy import (
    Column, String, Boolean, DateTime,
    SmallInteger, Numeric, ForeignKey
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from database import Base


class Organization(Base):
    __tablename__ = "organization_master"

    organization_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    # ── NAME ───────────────────────────────────────────────
    organization_name_raw = Column(String(300), nullable=True)
    organization_name_normalized = Column(String(300), nullable=True)

    # ── ADDRESS ────────────────────────────────────────────
    address_line_1 = Column(String(200), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(2), nullable=True)
    zip = Column(String(5), nullable=True)

    # ── CLASSIFICATION ─────────────────────────────────────
    organization_type = Column(String(30), nullable=True)
    solo_practice_flag = Column(Boolean, default=False)
    large_system_flag = Column(Boolean, default=False)
    practice_size_estimate = Column(SmallInteger, nullable=True)

    # ── STABILITY TRACKING ─────────────────────────────────
    name_change_count = Column(SmallInteger, default=0)
    first_seen_date = Column(DateTime(timezone=True), nullable=True)
    last_seen_date = Column(DateTime(timezone=True), nullable=True)

    # ── CONFIDENCE ─────────────────────────────────────────
    confidence_score = Column(Numeric(5, 2), nullable=True)
    source = Column(String(50), default="nppes_clustering")

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

    # ── RELATIONSHIPS ──────────────────────────────────────
    physician_links = relationship(
        "PhysicianOrganizationLink",
        back_populates="organization",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Organization {self.organization_name_normalized}>"


class PhysicianOrganizationLink(Base):
    __tablename__ = "physician_organization_link"

    link_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    npi = Column(
        String(10),
        ForeignKey("physician.npi", ondelete="CASCADE"),
        nullable=False
    )
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organization_master.organization_id"),
        nullable=False
    )

    link_type = Column(String(30), nullable=True)
    source = Column(String(50), nullable=True)
    confidence_score = Column(Numeric(5, 2), nullable=True)
    org_phone_verified = Column(Boolean, default=False)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    # ── RELATIONSHIPS ──────────────────────────────────────
    physician = relationship(
        "Physician",
        back_populates="organization_links"
    )
    organization = relationship(
        "Organization",
        back_populates="physician_links"
    )

    def __repr__(self):
        return f"<PhysicianOrgLink npi={self.npi}>"