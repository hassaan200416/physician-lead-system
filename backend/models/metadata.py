# models/metadata.py
# The audit trail for the entire system.
# Every field value change is recorded here with
# full provenance — source, timestamp, confidence.

from sqlalchemy import (
    Column, String, Boolean, DateTime,
    Numeric, Text, ForeignKey
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from database import Base


class FieldValueHistory(Base):
    __tablename__ = "field_value_history"

    history_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    # ── ENTITY REFERENCE ───────────────────────────────────
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(50), nullable=False)

    # Links back to physician when entity_type = 'physician'
    # nullable=True because entity could also be org or license
    npi = Column(
        String(10),
        ForeignKey("physician.npi", ondelete="CASCADE"),
        nullable=True
    )

    # ── FIELD DATA ─────────────────────────────────────────
    field_name = Column(String(100), nullable=False)
    field_value = Column(Text, nullable=True)

    # ── SOURCE ─────────────────────────────────────────────
    source_name = Column(String(50), nullable=True)

    # ── TIMESTAMPS ─────────────────────────────────────────
    # collected_timestamp: when first acquired from source
    # last_validated_timestamp: when last confirmed still accurate
    # These are different — never confuse them
    collected_timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now()
    )
    last_validated_timestamp = Column(
        DateTime(timezone=True),
        nullable=True
    )

    # ── QUALITY ────────────────────────────────────────────
    confidence_score = Column(Numeric(5, 2), nullable=True)
    # Allowed values: valid, suspicious, unverified, failed
    validation_status = Column(String(20), nullable=True)

    # ── VERSIONING ─────────────────────────────────────────
    # Only one row per entity+field should have is_current=True
    is_current = Column(Boolean, default=True)

    # Chain of custody — points to the newer row that
    # replaced this one when a value is updated
    superseded_by = Column(
        UUID(as_uuid=True),
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    # ── RELATIONSHIPS ──────────────────────────────────────
    physician = relationship(
        "Physician",
        back_populates="field_history"
    )

    def __repr__(self):
        return (
            f"<FieldValueHistory "
            f"entity={self.entity_type}:{self.entity_id} "
            f"field={self.field_name}>"
        )