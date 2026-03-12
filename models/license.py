# models/license.py
# Medical license records — one physician can have
# licenses in multiple states, so stored separately.

from sqlalchemy import (
    Column, String, Boolean, DateTime,
    ForeignKey
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from database import Base


class License(Base):
    __tablename__ = "license"

    license_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    npi = Column(
        String(10),
        ForeignKey("physician.npi", ondelete="CASCADE"),
        nullable=False
    )

    # ── LICENSE DATA ───────────────────────────────────────
    license_number = Column(String(50), nullable=True)
    license_state = Column(String(2), nullable=True)
    linked_taxonomy_code = Column(String(20), nullable=True)

    # ── FLAGS ──────────────────────────────────────────────
    is_primary_license = Column(Boolean, default=False)

    # ── VALIDATION ─────────────────────────────────────────
    # Allowed values: unverified, heuristic_pass,
    # board_verified, board_failed, format_suspicious
    verification_status = Column(
        String(20),
        default="unverified"
    )
    format_valid = Column(Boolean, nullable=True)

    # ── TRACKING ───────────────────────────────────────────
    source = Column(String(30), default="nppes")
    last_seen_date = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    # ── RELATIONSHIPS ──────────────────────────────────────
    physician = relationship(
        "Physician",
        back_populates="licenses"
    )

    def __repr__(self):
        return (
            f"<License npi={self.npi} "
            f"state={self.license_state} "
            f"number={self.license_number}>"
        )