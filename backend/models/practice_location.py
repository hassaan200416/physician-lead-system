# models/practice_location.py
# Stores all practice location addresses for each physician.
# One physician can have multiple locations.

from sqlalchemy import (
    Column, String, Boolean, DateTime,
    SmallInteger, Numeric, ForeignKey
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from database import Base


class PracticeLocation(Base):
    __tablename__ = "physician_practice_locations"

    location_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    npi = Column(
        String(10),
        ForeignKey("physician.npi", ondelete="CASCADE"),
        nullable=False
    )

    # ── ADDRESS COMPONENTS ─────────────────────────────────
    address_line_1 = Column(String(200), nullable=True)
    address_line_2 = Column(String(200), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(2), nullable=True)
    zip = Column(String(5), nullable=True)
    zip_plus4 = Column(String(4), nullable=True)

    # ── GEOCODING (schema ready, not populated at launch) ──
    latitude = Column(Numeric(9, 6), nullable=True)
    longitude = Column(Numeric(9, 6), nullable=True)
    geocoded_at = Column(DateTime(timezone=True), nullable=True)

    # ── CLASSIFICATION ─────────────────────────────────────
    is_primary_location = Column(Boolean, default=False)
    is_mailing_address = Column(Boolean, default=False)
    location_type = Column(String(30), nullable=True)

    # ── CLUSTERING ─────────────────────────────────────────
    practice_cluster_id = Column(UUID(as_uuid=True), nullable=True)

    # ── CONFIDENCE AND TRACKING ────────────────────────────
    address_confidence_score = Column(Numeric(5, 2), nullable=True)
    address_change_count = Column(SmallInteger, default=0)
    address_stability_score = Column(Numeric(3, 2), default=1.0)
    address_last_updated = Column(DateTime(timezone=True), nullable=True)
    address_last_seen = Column(DateTime(timezone=True), nullable=True)

    # ── VALIDATION ─────────────────────────────────────────
    usps_validated = Column(Boolean, default=False)
    usps_validated_at = Column(DateTime(timezone=True), nullable=True)

    # ── SOURCE ─────────────────────────────────────────────
    source_name = Column(String(50), default="nppes")
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    # ── RELATIONSHIPS ──────────────────────────────────────
    physician = relationship(
        "Physician",
        back_populates="practice_locations"
    )

    def __repr__(self):
        return (
            f"<PracticeLocation npi={self.npi} "
            f"{self.address_line_1}, {self.city}, {self.state}>"
        )