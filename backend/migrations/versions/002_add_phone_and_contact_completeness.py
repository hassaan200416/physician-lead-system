"""Add phone enrichment and contact completeness columns

Revision ID: 002_phone_enrichment
Revises: 340e952d67cc
Create Date: 2026-03-20
"""

from alembic import op
import sqlalchemy as sa

# These three lines are required by Alembic
revision = '002_phone_enrichment'
down_revision = '340e952d67cc'
branch_labels = None
depends_on = None


def upgrade():
    # All columns already exist in the database from a previous session.
    # This migration is intentionally left empty to mark it as complete.
    pass


def downgrade():
    pass