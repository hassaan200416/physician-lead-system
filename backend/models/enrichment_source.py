# models/enrichment_source.py
#
# Tracks cumulative statistics for every enrichment tool used in the pipeline.
# One row per tool. Updated atomically at the end of each tool's run.
#
# Purpose
# -------
# Gives a live dashboard of what each tool has contributed:
#   - How many emails and phones it found
#   - How many physicians it was run against
#   - Its hit rate (total_hits / total_attempts)
#   - When it was last used
#
# How to update stats (pattern used by all enrichment scripts)
# ------------------------------------------------------------
#   UPDATE enrichment_source_stats SET
#       emails_provided  = emails_provided  + :new_emails,
#       phones_provided  = phones_provided  + :new_phones,
#       total_hits       = total_hits       + :new_hits,
#       total_attempts   = total_attempts   + :new_attempts,
#       last_used_at     = :now,
#       updated_at       = :now
#   WHERE source_name = :source_name;
#
# All increments are additive - never set absolute values.
# This ensures concurrent runs from multiple scripts do not
# overwrite each other's counts.
#
# Source name registry
# --------------------
# The following source_name values are seeded by migration 003
# and must be used exactly (case-sensitive) in all enrichment scripts:
#
#   'hunter.io'     Hunter.io Email Finder API
#   'contactout'    ContactOut (API or manual CSV import)
#   'apollo'        Apollo.io (API)
#   'fullenrich'    FullEnrich (API)
#   'lusha'         Lusha (API or manual)
#   'snov.io'       Snov.io (API)
#   'rocketreach'   RocketReach (API or manual)
#   'bettercontact' BetterContact (API)
#
# Add new sources by inserting a row into this table at runtime.
# Do NOT hardcode source names anywhere except this file and the
# enrichment script that owns that source.

from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.sql import func

from database import Base


class EnrichmentSourceStats(Base):
    __tablename__ = "enrichment_source_stats"

    # -- PRIMARY KEY --
    # Canonical tool identifier. Must match strings written into
    # physician.enrichment_sources and leads.enrichment_sources arrays.
    source_name = Column(String(50), primary_key=True, nullable=False)

    # -- COUNTERS --
    # All counters are cumulative and additive.
    # Never set to an absolute value - always increment with +=.

    emails_provided = Column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
        doc="Cumulative count of personal_email values delivered by this source.",
    )
    phones_provided = Column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
        doc="Cumulative count of mobile_phone values delivered by this source.",
    )
    total_hits = Column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
        doc="Physicians where this source returned ANY data (email or phone). "
        "Hit rate = total_hits / total_attempts.",
    )
    total_attempts = Column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
        doc="Total physicians this source was called for, hit or miss. "
        "Includes physicians where no data was found.",
    )

    # -- TIMESTAMPS --
    last_used_at = Column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp of the most recent completed tool run. "
        "Null if this source has never been run.",
    )
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Application must update this on every stats write.",
    )

    def __repr__(self) -> str:
        attempts = getattr(self, "total_attempts", 0)
        hits = getattr(self, "total_hits", 0)
        emails = getattr(self, "emails_provided", 0)
        phones = getattr(self, "phones_provided", 0)

        hit_rate = (
            f"{hits / attempts * 100:.1f}%"
            if isinstance(attempts, (int, float)) and attempts > 0
            else "n/a"
        )
        return (
            f"<EnrichmentSourceStats source={self.source_name} "
            f"emails={emails} phones={phones} "
            f"hit_rate={hit_rate}>"
        )
