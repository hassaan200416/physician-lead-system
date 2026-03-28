# models/__init__.py
# Import all models here so SQLAlchemy registers them
# before Alembic runs migrations.
# Order matters — Physician must come before tables
# that reference it via foreign key.

from models.physician import Physician
from models.practice_location import PracticeLocation
from models.organization import Organization, PhysicianOrganizationLink
from models.license import License
from models.metadata import FieldValueHistory
from models.enrichment_source import EnrichmentSourceStats

__all__ = [
    "Physician",
    "PracticeLocation",
    "Organization",
    "PhysicianOrganizationLink",
    "License",
    "FieldValueHistory",
    "EnrichmentSourceStats",
]
